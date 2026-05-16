package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KeelPoolSpec defines the desired state of KeelPool.
type KeelPoolSpec struct {
	// Image is the KEEL container image. Default: ghcr.io/virtlabs/keel:latest
	// +optional
	// +kubebuilder:default="ghcr.io/virtlabs/keel:latest"
	Image string `json:"image,omitempty"`

	// Replicas is the number of KEEL proxy instances.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas,omitempty"`

	// ClusterMode enables gossip-based HA across all replicas.
	// When true a StatefulSet is rendered and INITIAL_PEERS is auto-generated.
	// +optional
	ClusterMode bool `json:"clusterMode,omitempty"`

	// Protocol is the frontend wire protocol.
	// +kubebuilder:validation:Enum=postgres;mysql
	Protocol string `json:"protocol"`

	// ListenPort is the port KEEL binds for client connections.
	// +optional
	// +kubebuilder:default=7432
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	ListenPort int32 `json:"listenPort,omitempty"`

	// MinPoolSize is the minimum backend connections per worker.
	// +optional
	// +kubebuilder:default=5
	MinPoolSize int32 `json:"minPoolSize,omitempty"`

	// MaxPoolSize is the maximum backend connections per worker.
	// +optional
	// +kubebuilder:default=50
	MaxPoolSize int32 `json:"maxPoolSize,omitempty"`

	// MaxConnectionAgeSeconds forcibly closes backend connections older than
	// this value. 0 disables connection aging.
	// +optional
	MaxConnectionAgeSeconds int64 `json:"maxConnectionAgeSeconds,omitempty"`

	// AuthMethod is the client authentication method.
	// +optional
	// +kubebuilder:default="scram-sha-256"
	// +kubebuilder:validation:Enum=scram-sha-256;md5;trust;ldap;pam;cert;reject
	AuthMethod string `json:"authMethod,omitempty"`

	// AuthConfig holds method-specific authentication parameters (LDAP URL,
	// PAM service name, auth_query SQL, etc.).
	// +optional
	AuthConfig map[string]string `json:"authConfig,omitempty"`

	// Backends is the list of upstream database servers.
	// +kubebuilder:validation:MinItems=1
	Backends []KeelBackend `json:"backends"`

	// TLS configures frontend TLS termination.
	// +optional
	TLS *KeelTLSConfig `json:"tls,omitempty"`

	// Tracing configures OTLP distributed tracing export.
	// +optional
	Tracing *KeelTracingConfig `json:"tracing,omitempty"`

	// Resources sets CPU/memory requests and limits for the KEEL container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// KeelBackend describes one upstream database server.
type KeelBackend struct {
	// Host is the database hostname or IP.
	Host string `json:"host"`

	// Port is the database port.
	// +optional
	// +kubebuilder:default=5432
	Port int32 `json:"port,omitempty"`

	// Database is the default database name.
	// +optional
	// +kubebuilder:default=postgres
	Database string `json:"database,omitempty"`

	// Username is the backend connection user.
	// +optional
	// +kubebuilder:default=postgres
	Username string `json:"username,omitempty"`

	// PasswordSecret is a reference to a K8s Secret key holding the password.
	// +optional
	PasswordSecret *corev1.SecretKeySelector `json:"passwordSecret,omitempty"`

	// Role is the routing role hint (RW=primary, RO=replica).
	// +optional
	// +kubebuilder:default=RW
	// +kubebuilder:validation:Enum=RW;RO;ANY
	Role string `json:"role,omitempty"`

	// Weight is the load-balancing weight.
	// +optional
	// +kubebuilder:default=100
	// +kubebuilder:validation:Minimum=1
	Weight int32 `json:"weight,omitempty"`
}

// KeelTLSConfig configures frontend TLS.
type KeelTLSConfig struct {
	// Enabled turns on TLS for client connections.
	Enabled bool `json:"enabled"`

	// AutoGenerate uses the built-in CA to create leaf certs on startup.
	// +optional
	AutoGenerate bool `json:"autoGenerate,omitempty"`

	// SecretName is a K8s TLS Secret (tls.crt + tls.key).
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// CACertSecret is a Secret containing a CA certificate bundle.
	// +optional
	CACertSecret string `json:"caCertSecret,omitempty"`
}

// KeelTracingConfig configures OTLP distributed tracing.
type KeelTracingConfig struct {
	// Enabled turns on tracing.
	Enabled bool `json:"enabled"`

	// Endpoint is the OTLP/HTTP collector URL.
	// Example: http://tempo:4318/v1/traces
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// SampleRatePpm is the trace sample rate in parts-per-million.
	// 1000000 = 100%, 10000 = 1%.
	// +optional
	// +kubebuilder:default=10000
	SampleRatePpm int32 `json:"sampleRatePpm,omitempty"`
}

// KeelPoolStatus defines the observed state of KeelPool.
type KeelPoolStatus struct {
	// Phase is the high-level reconciliation phase.
	// +optional
	Phase string `json:"phase,omitempty"`

	// ReadyReplicas is the number of ready KEEL proxy pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// ObservedGeneration is the last .metadata.generation the controller acted on.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Message is a human-readable status detail.
	// +optional
	Message string `json:"message,omitempty"`

	// Conditions contains standard Kubernetes condition entries.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// KeelPool is the Schema for the keelpools API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.readyReplicas
// +kubebuilder:printcolumn:name="Protocol",type=string,JSONPath=".spec.protocol"
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=".spec.replicas"
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=".status.readyReplicas"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type KeelPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KeelPoolSpec   `json:"spec,omitempty"`
	Status KeelPoolStatus `json:"status,omitempty"`
}

// KeelPoolList contains a list of KeelPool.
// +kubebuilder:object:root=true
type KeelPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KeelPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KeelPool{}, &KeelPoolList{})
}

// DefaultCPURequest is the default CPU resource request.
var DefaultCPURequest = resource.MustParse("250m")

// DefaultMemoryRequest is the default memory resource request.
var DefaultMemoryRequest = resource.MustParse("128Mi")

// DefaultCPULimit is the default CPU resource limit.
var DefaultCPULimit = resource.MustParse("2")

// DefaultMemoryLimit is the default memory resource limit.
var DefaultMemoryLimit = resource.MustParse("512Mi")
