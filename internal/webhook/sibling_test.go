/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

const (
	testNS = "myns"
)

// scheme builds a runtime scheme with both core and jetstream types so the
// fake client can store and list them. Done lazily per-test to avoid global
// state leak between tests.
func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, api.AddToScheme(s))
	return s
}

// newNamespace builds a corev1.Namespace with the given drill-active value
// (empty means the annotation is absent).
func newNamespace(name, drillID string) *corev1.Namespace {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
	if drillID != "" {
		ns.Annotations = map[string]string{DrillActiveAnnotation: drillID}
	}
	return ns
}

func newStream(metaName, specName string, mirror *api.StreamSource) *api.Stream {
	return &api.Stream{
		ObjectMeta: metav1.ObjectMeta{Name: metaName, Namespace: testNS},
		Spec:       api.StreamSpec{Name: specName, Mirror: mirror},
	}
}

func newKV(metaName, bucket string) *api.KeyValue {
	return &api.KeyValue{
		ObjectMeta: metav1.ObjectMeta{Name: metaName, Namespace: testNS},
		Spec:       api.KeyValueSpec{Bucket: bucket},
	}
}

func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(objs...).
		Build()
}

func TestStreamValidator_NoSibling_Allow(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, ""))
	v := &StreamValidator{Client: c}
	self := newStream("orders-primary", "ORDERS", nil)
	_, err := v.ValidateCreate(context.Background(), self)
	require.NoError(t, err)
}

func TestStreamValidator_SiblingSameSpecName_Reject(t *testing.T) {
	existing := newStream("orders-primary", "ORDERS", nil)
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &StreamValidator{Client: c}

	// new CR also wants spec.name=ORDERS (mirror of itself, classic drift)
	self := newStream("orders-mirror", "ORDERS", &api.StreamSource{Name: "ORDERS"})
	_, err := v.ValidateCreate(context.Background(), self)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ORDERS")
	require.Contains(t, err.Error(), "rejected by sibling-conflict webhook")
	require.Contains(t, err.Error(), DrillActiveAnnotation)
}

func TestStreamValidator_SiblingMirrorsSelfSpecName_Reject(t *testing.T) {
	// Sibling CR is a *mirror* of "ORDERS" (post-drift recreation).
	mirror := newStream("orders-mirror", "ORDERS-MIRROR", &api.StreamSource{Name: "ORDERS"})
	c := newFakeClient(t, newNamespace(testNS, ""), mirror)
	v := &StreamValidator{Client: c}

	// Self is the primary "ORDERS" — sibling's mirror.name == self.name.
	self := newStream("orders-primary", "ORDERS", nil)
	_, err := v.ValidateCreate(context.Background(), self)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mirror")
	require.Contains(t, err.Error(), "ORDERS")
}

func TestStreamValidator_SelfMirrorsSiblingSpecName_Reject(t *testing.T) {
	// Sibling primary already exists.
	primary := newStream("orders-primary", "ORDERS", nil)
	c := newFakeClient(t, newNamespace(testNS, ""), primary)
	v := &StreamValidator{Client: c}

	// Self is a mirror of "ORDERS" — the bug from 2026-05-24 incident.
	self := newStream("orders-mirror", "ORDERS-MIRROR", &api.StreamSource{Name: "ORDERS"})
	_, err := v.ValidateCreate(context.Background(), self)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mirror")
}

func TestStreamValidator_DrillActive_AllowWithWarning(t *testing.T) {
	existing := newStream("orders-primary", "ORDERS", nil)
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-24-abc"), existing)
	v := &StreamValidator{Client: c}

	self := newStream("orders-mirror", "ORDERS", &api.StreamSource{Name: "ORDERS"})
	warnings, err := v.ValidateCreate(context.Background(), self)
	require.NoError(t, err)
	require.NotEmpty(t, warnings, "expected a warning when drill-active overrides the conflict")
	require.Contains(t, strings.Join(warnings, "\n"), "drill-2026-05-24-abc")
}

func TestStreamValidator_UpdateSelf_NoFalsePositive(t *testing.T) {
	// The only CR named "orders-primary" with spec.name=ORDERS — and we're
	// updating it. The list-and-filter logic must exclude self, otherwise
	// every UPDATE would self-reject.
	self := newStream("orders-primary", "ORDERS", nil)
	c := newFakeClient(t, newNamespace(testNS, ""), self)
	v := &StreamValidator{Client: c}

	updated := newStream("orders-primary", "ORDERS", nil)
	updated.Spec.Description = "updated"
	_, err := v.ValidateUpdate(context.Background(), self, updated)
	require.NoError(t, err)
}

func TestStreamValidator_EmptySpecName_NoFalsePositive(t *testing.T) {
	// Two CRs with spec.name="" should NOT conflict (defensive: empty name
	// is a different kind of error, surfaced elsewhere).
	a := newStream("a", "", nil)
	b := newStream("b", "", nil)
	c := newFakeClient(t, newNamespace(testNS, ""), a)
	v := &StreamValidator{Client: c}

	_, err := v.ValidateCreate(context.Background(), b)
	require.NoError(t, err)
}

func TestKeyValueValidator_NoSibling_Allow(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, ""))
	v := &KeyValueValidator{Client: c}
	self := newKV("config-primary", "CONFIG")
	_, err := v.ValidateCreate(context.Background(), self)
	require.NoError(t, err)
}

func TestKeyValueValidator_SiblingSameBucket_Reject(t *testing.T) {
	existing := newKV("config-primary", "CONFIG")
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &KeyValueValidator{Client: c}

	self := newKV("config-mirror", "CONFIG")
	_, err := v.ValidateCreate(context.Background(), self)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CONFIG")
}

func TestKeyValueValidator_DrillActive_AllowWithWarning(t *testing.T) {
	existing := newKV("config-primary", "CONFIG")
	c := newFakeClient(t, newNamespace(testNS, "drill-xyz"), existing)
	v := &KeyValueValidator{Client: c}

	self := newKV("config-mirror", "CONFIG")
	warnings, err := v.ValidateCreate(context.Background(), self)
	require.NoError(t, err)
	require.NotEmpty(t, warnings)
	require.Contains(t, strings.Join(warnings, "\n"), "drill-xyz")
}

func TestKeyValueValidator_UpdateSelf_NoFalsePositive(t *testing.T) {
	self := newKV("config-primary", "CONFIG")
	c := newFakeClient(t, newNamespace(testNS, ""), self)
	v := &KeyValueValidator{Client: c}

	updated := newKV("config-primary", "CONFIG")
	updated.Spec.Description = "updated"
	_, err := v.ValidateUpdate(context.Background(), self, updated)
	require.NoError(t, err)
}

func TestStreamValidator_NamespaceLookupNotFound_StillReject(t *testing.T) {
	// Namespace object missing from cluster (edge case): the conflict
	// detection still fires because findStreamConflict only depends on
	// the Stream list; only the override path needs the namespace.
	existing := newStream("orders-primary", "ORDERS", nil)
	c := newFakeClient(t, existing) // no namespace object
	v := &StreamValidator{Client: c}

	self := newStream("orders-mirror", "ORDERS", &api.StreamSource{Name: "ORDERS"})
	_, err := v.ValidateCreate(context.Background(), self)
	require.Error(t, err)
}

func TestStreamValidator_ValidateDelete_AlwaysAllow(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, ""))
	v := &StreamValidator{Client: c}
	_, err := v.ValidateDelete(context.Background(), newStream("any", "ANY", nil))
	require.NoError(t, err)
}
