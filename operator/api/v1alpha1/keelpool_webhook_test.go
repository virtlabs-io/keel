package v1alpha1_test

import (
	"testing"

	"github.com/virtlabs-io/keel-operator/api/v1alpha1"
)

func makePool(protocol string, backends []v1alpha1.KeelBackend) *v1alpha1.KeelPool {
	return &v1alpha1.KeelPool{
		Spec: v1alpha1.KeelPoolSpec{
			Protocol:    protocol,
			Backends:    backends,
			Replicas:    1,
			ListenPort:  7432,
			MinPoolSize: 5,
			MaxPoolSize: 50,
			AuthMethod:  "scram-sha-256",
		},
	}
}

func goodBackend() v1alpha1.KeelBackend {
	return v1alpha1.KeelBackend{Host: "postgres.default.svc.cluster.local", Port: 5432}
}

func TestDefaulter(t *testing.T) {
	pool := &v1alpha1.KeelPool{}
	pool.Default()

	if pool.Spec.Image == "" {
		t.Error("Default() did not set Image")
	}
	if pool.Spec.Replicas != 1 {
		t.Errorf("Default() Replicas = %d, want 1", pool.Spec.Replicas)
	}
	if pool.Spec.ListenPort != 7432 {
		t.Errorf("Default() ListenPort = %d, want 7432", pool.Spec.ListenPort)
	}
	if pool.Spec.MinPoolSize != 5 {
		t.Errorf("Default() MinPoolSize = %d, want 5", pool.Spec.MinPoolSize)
	}
	if pool.Spec.MaxPoolSize != 50 {
		t.Errorf("Default() MaxPoolSize = %d, want 50", pool.Spec.MaxPoolSize)
	}
	if pool.Spec.AuthMethod != "scram-sha-256" {
		t.Errorf("Default() AuthMethod = %q, want scram-sha-256", pool.Spec.AuthMethod)
	}
}

func TestValidateCreate_Valid(t *testing.T) {
	pool := makePool("postgres", []v1alpha1.KeelBackend{goodBackend()})
	if _, err := pool.ValidateCreate(); err != nil {
		t.Errorf("unexpected error for valid spec: %v", err)
	}
}

func TestValidateCreate_MissingProtocol(t *testing.T) {
	pool := makePool("", []v1alpha1.KeelBackend{goodBackend()})
	_, err := pool.ValidateCreate()
	if err == nil {
		t.Error("expected error for missing protocol")
	}
}

func TestValidateCreate_InvalidProtocol(t *testing.T) {
	pool := makePool("redis", []v1alpha1.KeelBackend{goodBackend()})
	_, err := pool.ValidateCreate()
	if err == nil {
		t.Error("expected error for invalid protocol")
	}
}

func TestValidateCreate_NoBackends(t *testing.T) {
	pool := makePool("postgres", nil)
	_, err := pool.ValidateCreate()
	if err == nil {
		t.Error("expected error for empty backends")
	}
}

func TestValidateCreate_MissingBackendHost(t *testing.T) {
	pool := makePool("postgres", []v1alpha1.KeelBackend{{Host: "", Port: 5432}})
	_, err := pool.ValidateCreate()
	if err == nil {
		t.Error("expected error for backend with empty host")
	}
}

func TestValidateCreate_InvalidBackendPort(t *testing.T) {
	pool := makePool("postgres", []v1alpha1.KeelBackend{{Host: "db", Port: 99999}})
	_, err := pool.ValidateCreate()
	if err == nil {
		t.Error("expected error for backend port out of range")
	}
}

func TestValidateCreate_MinGTMax(t *testing.T) {
	pool := makePool("postgres", []v1alpha1.KeelBackend{goodBackend()})
	pool.Spec.MinPoolSize = 100
	pool.Spec.MaxPoolSize = 10
	_, err := pool.ValidateCreate()
	if err == nil {
		t.Error("expected error when minPoolSize > maxPoolSize")
	}
}

func TestValidateCreate_InvalidAuthMethod(t *testing.T) {
	pool := makePool("postgres", []v1alpha1.KeelBackend{goodBackend()})
	pool.Spec.AuthMethod = "kerberos"
	_, err := pool.ValidateCreate()
	if err == nil {
		t.Error("expected error for unknown authMethod")
	}
}

func TestValidateCreate_MySQL(t *testing.T) {
	pool := makePool("mysql", []v1alpha1.KeelBackend{{Host: "mysql.default.svc", Port: 3306}})
	if _, err := pool.ValidateCreate(); err != nil {
		t.Errorf("unexpected error for valid mysql spec: %v", err)
	}
}

func TestValidateDelete_AlwaysAllowed(t *testing.T) {
	pool := makePool("postgres", []v1alpha1.KeelBackend{goodBackend()})
	if _, err := pool.ValidateDelete(); err != nil {
		t.Errorf("ValidateDelete should always succeed, got: %v", err)
	}
}
