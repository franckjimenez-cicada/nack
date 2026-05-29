/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"testing"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestTranslateStreamSpecToMirror(t *testing.T) {
	orig := &api.StreamSpec{
		Name:     "activitylog",
		Subjects: []string{"cobactions", "activitylog.start", "activitylog.end"},
		Retention: "limits",
		Replicas:  3,
	}
	got := translateStreamSpecToMirror(orig, "dev-2nd-east")

	if got.Subjects != nil {
		t.Errorf("Subjects should be cleared, got %v", got.Subjects)
	}
	if got.Mirror == nil {
		t.Fatal("Mirror should be set")
	}
	if got.Mirror.Name != "activitylog" {
		t.Errorf("Mirror.Name = %q, want %q", got.Mirror.Name, "activitylog")
	}
	if got.Mirror.ExternalAPIPrefix != "$JS.dev-2nd-east.API" {
		t.Errorf("Mirror.ExternalAPIPrefix = %q, want %q",
			got.Mirror.ExternalAPIPrefix, "$JS.dev-2nd-east.API")
	}
	if got.Mirror.ExternalDeliverPrefix != "deliver.activitylog.dr" {
		t.Errorf("Mirror.ExternalDeliverPrefix = %q, want %q",
			got.Mirror.ExternalDeliverPrefix, "deliver.activitylog.dr")
	}

	// Original must be untouched.
	if len(orig.Subjects) != 3 {
		t.Errorf("Original Subjects was mutated: %v", orig.Subjects)
	}
	if orig.Mirror != nil {
		t.Errorf("Original Mirror was mutated: %v", orig.Mirror)
	}

	// Retention + Replicas (and other primary-form fields) survive on the
	// translated spec; NATS server will ignore them in mirror mode but we
	// don't strip aggressively — keeping them simplifies revert.
	if got.Replicas != 3 {
		t.Errorf("Replicas should be preserved, got %d", got.Replicas)
	}
}

func TestTranslateKeyValueSpecToMirror(t *testing.T) {
	orig := &api.KeyValueSpec{
		Bucket:   "consumer-offsets",
		Replicas: 3,
	}
	got := translateKeyValueSpecToMirror(orig, "dev-2nd-east")

	if got.Mirror == nil {
		t.Fatal("Mirror should be set")
	}
	if got.Mirror.Name != "KV_consumer-offsets" {
		t.Errorf("Mirror.Name = %q, want %q", got.Mirror.Name, "KV_consumer-offsets")
	}
	if got.Mirror.ExternalAPIPrefix != "$JS.dev-2nd-east.API" {
		t.Errorf("Mirror.ExternalAPIPrefix = %q, want %q",
			got.Mirror.ExternalAPIPrefix, "$JS.dev-2nd-east.API")
	}
	if got.Mirror.ExternalDeliverPrefix != "deliver.kv.consumer-offsets.dr" {
		t.Errorf("Mirror.ExternalDeliverPrefix = %q, want %q",
			got.Mirror.ExternalDeliverPrefix, "deliver.kv.consumer-offsets.dr")
	}

	// Original must be untouched.
	if orig.Mirror != nil {
		t.Errorf("Original Mirror was mutated: %v", orig.Mirror)
	}
}

// fakePassiveRoleGate is a minimal passiveRoleGate impl: the fake client
// + the two new accessors. No need to mock the rest of JetStreamController.
type fakePassiveRoleGate struct {
	client.Client
	enabled bool
	domain  string
}

func (f *fakePassiveRoleGate) PassiveRoleTranslationEnabled() bool { return f.enabled }
func (f *fakePassiveRoleGate) CrossRegionNATSDomain() string       { return f.domain }

func newFakeGate(t *testing.T, enabled bool, domain string, nsAnnotations map[string]string) *fakePassiveRoleGate {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "nats",
			Annotations: nsAnnotations,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).Build()
	return &fakePassiveRoleGate{Client: c, enabled: enabled, domain: domain}
}

func TestShouldTranslatePassiveRole(t *testing.T) {
	tests := []struct {
		name      string
		enabled   bool
		domain    string
		nsAnnots  map[string]string
		want      bool
		wantError bool
	}{
		{
			name:     "feature disabled",
			enabled:  false,
			domain:   "dev-2nd-east",
			nsAnnots: map[string]string{localRoleAnnotation: localRolePassive},
			want:     false,
		},
		{
			name:     "domain empty",
			enabled:  true,
			domain:   "",
			nsAnnots: map[string]string{localRoleAnnotation: localRolePassive},
			want:     false,
		},
		{
			name:     "annotation absent",
			enabled:  true,
			domain:   "dev-2nd-east",
			nsAnnots: nil,
			want:     false,
		},
		{
			name:     "annotation active",
			enabled:  true,
			domain:   "dev-2nd-east",
			nsAnnots: map[string]string{localRoleAnnotation: "active"},
			want:     false,
		},
		{
			name:     "annotation passive — all conditions met",
			enabled:  true,
			domain:   "dev-2nd-east",
			nsAnnots: map[string]string{localRoleAnnotation: localRolePassive},
			want:     true,
		},
		{
			name:     "annotation passive but other value casing",
			enabled:  true,
			domain:   "dev-2nd-east",
			nsAnnots: map[string]string{localRoleAnnotation: "Passive"},
			want:     false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jsc := newFakeGate(t, tc.enabled, tc.domain, tc.nsAnnots)
			got, err := shouldTranslatePassiveRole(context.Background(), jsc, "nats")
			if tc.wantError && err == nil {
				t.Fatalf("want error, got nil")
			}
			if !tc.wantError && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShouldTranslatePassiveRole_NamespaceNotFound(t *testing.T) {
	// Build a fake client with NO namespace objects — Get should return
	// NotFound and the helper should treat that as "not passive" rather
	// than erroring. This guards against the obvious failure mode of
	// "controller starts before ns exists" producing a spurious error.
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	jsc := &fakePassiveRoleGate{Client: c, enabled: true, domain: "dev-2nd-east"}

	got, err := shouldTranslatePassiveRole(context.Background(), jsc, "missing-ns")
	if err != nil {
		t.Fatalf("unexpected error on missing ns: %v", err)
	}
	if got {
		t.Errorf("missing ns should yield false, got true")
	}
}
