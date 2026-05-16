package v1alpha1

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var keelPoolLog = logf.Log.WithName("keelpool-webhook")

// SetupWebhookWithManager registers the defaulting and validation webhooks.
func (r *KeelPool) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-keel-virtlabs-io-v1alpha1-keelpool,mutating=true,failurePolicy=fail,sideEffects=None,groups=keel.virtlabs.io,resources=keelpools,verbs=create;update,versions=v1alpha1,name=mkeelpool.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &KeelPool{}

// Default applies default values to a newly-created KeelPool.
func (r *KeelPool) Default() {
	keelPoolLog.Info("defaulting", "name", r.Name)

	if r.Spec.Image == "" {
		r.Spec.Image = "ghcr.io/virtlabs-io/keel:latest"
	}
	if r.Spec.Replicas == 0 {
		r.Spec.Replicas = 1
	}
	if r.Spec.ListenPort == 0 {
		r.Spec.ListenPort = 7432
	}
	if r.Spec.MinPoolSize == 0 {
		r.Spec.MinPoolSize = 5
	}
	if r.Spec.MaxPoolSize == 0 {
		r.Spec.MaxPoolSize = 50
	}
	if r.Spec.AuthMethod == "" {
		r.Spec.AuthMethod = "scram-sha-256"
	}
}

// +kubebuilder:webhook:path=/validate-keel-virtlabs-io-v1alpha1-keelpool,mutating=false,failurePolicy=fail,sideEffects=None,groups=keel.virtlabs.io,resources=keelpools,verbs=create;update,versions=v1alpha1,name=vkeelpool.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &KeelPool{}

// ValidateCreate validates a KeelPool on creation.
func (r *KeelPool) ValidateCreate() (admission.Warnings, error) {
	keelPoolLog.Info("validate create", "name", r.Name)
	return nil, r.validateSpec()
}

// ValidateUpdate validates a KeelPool on update.
func (r *KeelPool) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	keelPoolLog.Info("validate update", "name", r.Name)
	return nil, r.validateSpec()
}

// ValidateDelete is a no-op — KeelPool deletions are always allowed.
func (r *KeelPool) ValidateDelete() (admission.Warnings, error) {
	keelPoolLog.Info("validate delete", "name", r.Name)
	return nil, nil
}

// validateSpec contains all business rules for a KeelPool spec.
func (r *KeelPool) validateSpec() error {
	var errs []error

	// protocol is required and must be postgres or mysql
	switch r.Spec.Protocol {
	case "postgres", "mysql":
		// valid
	case "":
		errs = append(errs, errors.New("spec.protocol is required"))
	default:
		errs = append(errs, fmt.Errorf("spec.protocol %q is invalid; must be postgres or mysql", r.Spec.Protocol))
	}

	// backends must have at least one entry
	if len(r.Spec.Backends) == 0 {
		errs = append(errs, errors.New("spec.backends must contain at least one entry"))
	}

	for i, b := range r.Spec.Backends {
		if b.Host == "" {
			errs = append(errs, fmt.Errorf("spec.backends[%d].host is required", i))
		}
		if b.Port < 1 || b.Port > 65535 {
			errs = append(errs, fmt.Errorf("spec.backends[%d].port %d is out of range [1,65535]", i, b.Port))
		}
	}

	// pool sizes
	if r.Spec.MinPoolSize < 0 {
		errs = append(errs, errors.New("spec.minPoolSize must be >= 0"))
	}
	if r.Spec.MaxPoolSize < 1 {
		errs = append(errs, errors.New("spec.maxPoolSize must be >= 1"))
	}
	if r.Spec.MinPoolSize > r.Spec.MaxPoolSize {
		errs = append(errs, fmt.Errorf("spec.minPoolSize (%d) must not exceed spec.maxPoolSize (%d)",
			r.Spec.MinPoolSize, r.Spec.MaxPoolSize))
	}

	// listen port
	if r.Spec.ListenPort < 1 || r.Spec.ListenPort > 65535 {
		errs = append(errs, fmt.Errorf("spec.listenPort %d is out of range [1,65535]", r.Spec.ListenPort))
	}

	// replicas
	if r.Spec.Replicas < 1 {
		errs = append(errs, errors.New("spec.replicas must be >= 1"))
	}

	// authMethod enum check
	switch r.Spec.AuthMethod {
	case "", "scram-sha-256", "md5", "trust", "ldap", "pam", "cert", "reject":
		// valid
	default:
		errs = append(errs, fmt.Errorf("spec.authMethod %q is not a recognised value", r.Spec.AuthMethod))
	}

	return errors.Join(errs...)
}
