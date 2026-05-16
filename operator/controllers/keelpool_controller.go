package controllers

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	keelv1alpha1 "github.com/virtlabs-io/keel-operator/api/v1alpha1"
)

// KeelPoolReconciler reconciles KeelPool objects.
type KeelPoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=keel.virtlabs.io,resources=keelpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=keel.virtlabs.io,resources=keelpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=keel.virtlabs.io,resources=keelpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps;services;secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile brings the cluster state for a KeelPool into the desired state.
func (r *KeelPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Reconciling KeelPool", "namespacedName", req.NamespacedName)

	// Fetch the KeelPool instance.
	pool := &keelv1alpha1.KeelPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Reconcile ConfigMap.
	if err := r.reconcileConfigMap(ctx, pool); err != nil {
		return ctrl.Result{}, r.setError(ctx, pool, "ConfigMapReconcileFailed", err)
	}

	// Reconcile Service.
	if err := r.reconcileService(ctx, pool); err != nil {
		return ctrl.Result{}, r.setError(ctx, pool, "ServiceReconcileFailed", err)
	}

	// Reconcile the workload (Deployment or StatefulSet).
	if pool.Spec.ClusterMode {
		if err := r.reconcileStatefulSet(ctx, pool); err != nil {
			return ctrl.Result{}, r.setError(ctx, pool, "StatefulSetReconcileFailed", err)
		}
	} else {
		if err := r.reconcileDeployment(ctx, pool); err != nil {
			return ctrl.Result{}, r.setError(ctx, pool, "DeploymentReconcileFailed", err)
		}
	}

	// Update status.
	return ctrl.Result{}, r.updateStatus(ctx, pool)
}

// ─── ConfigMap ──────────────────────────────────────────────────────────────

func (r *KeelPoolReconciler) reconcileConfigMap(ctx context.Context, pool *keelv1alpha1.KeelPool) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pool.Name,
			Namespace: pool.Namespace,
		},
	}
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Data = map[string]string{"keel.ini": buildINI(pool)}
		return ctrl.SetControllerReference(pool, cm, r.Scheme)
	})
	return err
}

// buildINI generates the keel.ini content from KeelPoolSpec.
func buildINI(pool *keelv1alpha1.KeelPool) string {
	var sb strings.Builder
	sb.WriteString("[keel]\nlog_level = info\n\n")

	proto := pool.Spec.Protocol
	if proto == "" {
		proto = "postgres"
	}
	port := pool.Spec.ListenPort
	if port == 0 {
		port = 7432
	}
	minPool := pool.Spec.MinPoolSize
	if minPool == 0 {
		minPool = 5
	}
	maxPool := pool.Spec.MaxPoolSize
	if maxPool == 0 {
		maxPool = 50
	}

	fmt.Fprintf(&sb, "[worker_group.default]\n")
	fmt.Fprintf(&sb, "name = default\n")
	fmt.Fprintf(&sb, "protocol = %s\n", proto)
	fmt.Fprintf(&sb, "bind_addr = 0.0.0.0\n")
	fmt.Fprintf(&sb, "bind_port = %d\n", port)
	fmt.Fprintf(&sb, "num_workers = 0\n")
	fmt.Fprintf(&sb, "min_pool_size = %d\n", minPool)
	fmt.Fprintf(&sb, "max_pool_size = %d\n", maxPool)
	if pool.Spec.MaxConnectionAgeSeconds > 0 {
		fmt.Fprintf(&sb, "max_connection_age_s = %d\n", pool.Spec.MaxConnectionAgeSeconds)
	}
	fmt.Fprintf(&sb, "probe = %s\n", proto)

	// Authentication
	if am := pool.Spec.AuthMethod; am != "" {
		fmt.Fprintf(&sb, "auth_method = %s\n", am)
	}
	if ac := pool.Spec.AuthConfig; len(ac) > 0 {
		for k, v := range ac {
			fmt.Fprintf(&sb, "%s = %s\n", k, v)
		}
	}
	sb.WriteString("\n")

	// Backends
	sb.WriteString("[worker_group.default.servers]\n")
	for i, be := range pool.Spec.Backends {
		bePort := be.Port
		if bePort == 0 {
			bePort = 5432
		}
		beDB := be.Database
		if beDB == "" {
			beDB = "postgres"
		}
		beUser := be.Username
		if beUser == "" {
			beUser = "postgres"
		}
		beRole := be.Role
		if beRole == "" {
			beRole = "RW"
		}
		name := fmt.Sprintf("backend_%d", i)
		// Password is injected via env var BACKEND_i_PASSWORD referenced as env:BACKEND_i_PASSWORD
		fmt.Fprintf(&sb, "%s = host=%s port=%d dbname=%s user=%s role=%s weight=%d",
			name, be.Host, bePort, beDB, beUser, beRole, be.Weight)
		if be.PasswordSecret != nil {
			envKey := fmt.Sprintf("BACKEND_%d_PASSWORD", i)
			fmt.Fprintf(&sb, " password=env:%s", envKey)
		}
		sb.WriteString("\n")
	}
	sb.WriteString("\n")

	// TLS
	if tls := pool.Spec.TLS; tls != nil && tls.Enabled {
		sb.WriteString("[tls]\n")
		if tls.AutoGenerate {
			sb.WriteString("tls_mode = auto\ntls_auto_generate = true\n")
		} else if tls.SecretName != "" {
			sb.WriteString("tls_mode = require\n")
			sb.WriteString("tls_cert_file = /etc/keel/tls/tls.crt\n")
			sb.WriteString("tls_key_file = /etc/keel/tls/tls.key\n")
		}
		sb.WriteString("\n")
	}

	// Tracing
	if tr := pool.Spec.Tracing; tr != nil && tr.Enabled && tr.Endpoint != "" {
		rate := tr.SampleRatePpm
		if rate == 0 {
			rate = 10000
		}
		sb.WriteString("[tracing]\n")
		sb.WriteString("enabled = true\n")
		fmt.Fprintf(&sb, "endpoint = %s\n", tr.Endpoint)
		fmt.Fprintf(&sb, "sample_rate_ppm = %d\n", rate)
		sb.WriteString("\n")
	}

	// Admin + Prometheus
	sb.WriteString("[admin]\nenabled = true\nlisten_addr = 0.0.0.0\nlisten_port = 6433\n\n")
	sb.WriteString("[prometheus]\nenabled = true\nlisten_addr = 0.0.0.0\nport = 9101\n")

	return sb.String()
}

// ─── Service ─────────────────────────────────────────────────────────────────

func (r *KeelPoolReconciler) reconcileService(ctx context.Context, pool *keelv1alpha1.KeelPool) error {
	listenPort := pool.Spec.ListenPort
	if listenPort == 0 {
		listenPort = 7432
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pool.Name,
			Namespace: pool.Namespace,
		},
	}
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Spec.Selector = map[string]string{
			"app.kubernetes.io/name":     "keel",
			"app.kubernetes.io/instance": pool.Name,
		}
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "proxy",
				Port:       listenPort,
				TargetPort: intstr.FromString("proxy"),
				Protocol:   corev1.ProtocolTCP,
			},
			{
				Name:       "admin",
				Port:       6433,
				TargetPort: intstr.FromString("admin"),
				Protocol:   corev1.ProtocolTCP,
			},
		}
		return ctrl.SetControllerReference(pool, svc, r.Scheme)
	})
	return err
}

// ─── Workload helpers ────────────────────────────────────────────────────────

func buildContainerSpec(pool *keelv1alpha1.KeelPool, envVars []corev1.EnvVar) corev1.Container {
	image := pool.Spec.Image
	if image == "" {
		image = "ghcr.io/virtlabs/keel:latest"
	}
	listenPort := pool.Spec.ListenPort
	if listenPort == 0 {
		listenPort = 7432
	}

	res := pool.Spec.Resources
	if res.Requests == nil {
		res.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    keelv1alpha1.DefaultCPURequest,
			corev1.ResourceMemory: keelv1alpha1.DefaultMemoryRequest,
		}
	}
	if res.Limits == nil {
		res.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    keelv1alpha1.DefaultCPULimit,
			corev1.ResourceMemory: keelv1alpha1.DefaultMemoryLimit,
		}
	}

	return corev1.Container{
		Name:            "keel",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Args:            []string{"-c", "/etc/keel/keel.ini"},
		Env:             envVars,
		Ports: []corev1.ContainerPort{
			{Name: "proxy", ContainerPort: listenPort, Protocol: corev1.ProtocolTCP},
			{Name: "admin", ContainerPort: 6433, Protocol: corev1.ProtocolTCP},
			{Name: "metrics", ContainerPort: 9101, Protocol: corev1.ProtocolTCP},
		},
		Resources: res,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: boolPtr(false),
			ReadOnlyRootFilesystem:   boolPtr(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/livez", Port: intstr.FromString("metrics")}},
			InitialDelaySeconds: 5, PeriodSeconds: 10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("metrics")}},
			InitialDelaySeconds: 3, PeriodSeconds: 5,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "config", MountPath: "/etc/keel", ReadOnly: true},
			{Name: "tmp", MountPath: "/tmp"},
			{Name: "run", MountPath: "/var/run/keel"},
		},
	}
}

func buildBackendEnvVars(pool *keelv1alpha1.KeelPool) []corev1.EnvVar {
	var envVars []corev1.EnvVar
	for i, be := range pool.Spec.Backends {
		if be.PasswordSecret != nil {
			envVars = append(envVars, corev1.EnvVar{
				Name: fmt.Sprintf("BACKEND_%d_PASSWORD", i),
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: be.PasswordSecret,
				},
			})
		}
	}
	return envVars
}

func buildVolumes(pool *keelv1alpha1.KeelPool) []corev1.Volume {
	vols := []corev1.Volume{
		{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: pool.Name}}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resourcePtr(resource.MustParse("64Mi"))}}},
		{Name: "run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resourcePtr(resource.MustParse("1Mi"))}}},
	}
	if tls := pool.Spec.TLS; tls != nil && tls.SecretName != "" {
		vols = append(vols, corev1.Volume{
			Name: "tls-certs",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: tls.SecretName},
			},
		})
	}
	return vols
}

func podLabels(pool *keelv1alpha1.KeelPool) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "keel",
		"app.kubernetes.io/instance": pool.Name,
	}
}

// ─── Deployment ──────────────────────────────────────────────────────────────

func (r *KeelPoolReconciler) reconcileDeployment(ctx context.Context, pool *keelv1alpha1.KeelPool) error {
	replicas := pool.Spec.Replicas
	if replicas == 0 {
		replicas = 1
	}
	envVars := buildBackendEnvVars(pool)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: pool.Name, Namespace: pool.Namespace},
	}
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Spec.Replicas = &replicas
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: podLabels(pool)}
		dep.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: podLabels(pool)},
			Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: boolPtr(true), RunAsUser: int64Ptr(65534)},
				Containers:      []corev1.Container{buildContainerSpec(pool, envVars)},
				Volumes:         buildVolumes(pool),
			},
		}
		return ctrl.SetControllerReference(pool, dep, r.Scheme)
	})
	return err
}

// ─── StatefulSet ─────────────────────────────────────────────────────────────

func (r *KeelPoolReconciler) reconcileStatefulSet(ctx context.Context, pool *keelv1alpha1.KeelPool) error {
	replicas := pool.Spec.Replicas
	if replicas == 0 {
		replicas = 3
	}

	// Build INITIAL_PEERS from all pod DNS names.
	clusterPort := int32(9100)
	peers := make([]string, replicas)
	for i := int32(0); i < replicas; i++ {
		peers[i] = fmt.Sprintf("%s-%d.%s-cluster.%s.svc.cluster.local:%d",
			pool.Name, i, pool.Name, pool.Namespace, clusterPort)
	}

	envVars := append(buildBackendEnvVars(pool),
		corev1.EnvVar{
			Name: "KEEL_CLUSTER_NODE_ID",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		corev1.EnvVar{
			Name:  "KEEL_CLUSTER_ENABLED",
			Value: "true",
		},
		corev1.EnvVar{
			Name:  "KEEL_CLUSTER_INITIAL_PEERS",
			Value: strings.Join(peers, ","),
		},
	)

	// Ensure the headless service exists for stable DNS.
	if err := r.reconcileHeadlessService(ctx, pool, clusterPort); err != nil {
		return err
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: pool.Name, Namespace: pool.Namespace},
	}
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.Spec.ServiceName = pool.Name + "-cluster"
		sts.Spec.Replicas = &replicas
		sts.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: podLabels(pool)}
		sts.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: podLabels(pool)},
			Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: boolPtr(true), RunAsUser: int64Ptr(65534)},
				Containers:      []corev1.Container{buildContainerSpec(pool, envVars)},
				Volumes:         buildVolumes(pool),
			},
		}
		return ctrl.SetControllerReference(pool, sts, r.Scheme)
	})
	return err
}

func (r *KeelPoolReconciler) reconcileHeadlessService(ctx context.Context, pool *keelv1alpha1.KeelPool, clusterPort int32) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: pool.Name + "-cluster", Namespace: pool.Namespace},
	}
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Spec.ClusterIP = "None"
		svc.Spec.Selector = podLabels(pool)
		svc.Spec.Ports = []corev1.ServicePort{
			{Name: "cluster", Port: clusterPort, TargetPort: intstr.FromString("cluster"), Protocol: corev1.ProtocolTCP},
		}
		return ctrl.SetControllerReference(pool, svc, r.Scheme)
	})
	return err
}

// ─── Status ──────────────────────────────────────────────────────────────────

func (r *KeelPoolReconciler) updateStatus(ctx context.Context, pool *keelv1alpha1.KeelPool) error {
	logger := log.FromContext(ctx)

	ready := int32(0)
	phase := "Pending"

	if pool.Spec.ClusterMode {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(pool), sts); err == nil {
			ready = sts.Status.ReadyReplicas
		}
	} else {
		dep := &appsv1.Deployment{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(pool), dep); err == nil {
			ready = dep.Status.ReadyReplicas
		}
	}

	desired := pool.Spec.Replicas
	if desired == 0 {
		desired = 1
	}
	if ready == desired {
		phase = "Running"
	} else if ready > 0 {
		phase = "Degraded"
	}

	patch := client.MergeFrom(pool.DeepCopy())
	pool.Status.Phase = phase
	pool.Status.ReadyReplicas = ready
	pool.Status.ObservedGeneration = pool.Generation

	if err := r.Status().Patch(ctx, pool, patch); err != nil {
		logger.Error(err, "Failed to patch KeelPool status")
		return err
	}
	return nil
}

func (r *KeelPoolReconciler) setError(ctx context.Context, pool *keelv1alpha1.KeelPool, reason string, err error) error {
	patch := client.MergeFrom(pool.DeepCopy())
	pool.Status.Phase = "Error"
	pool.Status.Message = fmt.Sprintf("%s: %v", reason, err)
	_ = r.Status().Patch(ctx, pool, patch)
	return err
}

// SetupWithManager registers the controller with the manager.
func (r *KeelPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&keelv1alpha1.KeelPool{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Complete(r)
}

// ─── Small helpers ───────────────────────────────────────────────────────────

func boolPtr(b bool) *bool             { return &b }
func int64Ptr(i int64) *int64          { return &i }
func resourcePtr(q resource.Quantity) *resource.Quantity { return &q }
