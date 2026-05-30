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

// newStreamWithAccount builds a Stream CR with the drp.cicada.io/nats-account
// label set. Used by account-aware sibling-check tests.
func newStreamWithAccount(metaName, specName, account string) *api.Stream {
	s := newStream(metaName, specName, nil)
	s.Labels = map[string]string{NATSAccountLabel: account}
	return s
}

func newKV(metaName, bucket string) *api.KeyValue {
	return &api.KeyValue{
		ObjectMeta: metav1.ObjectMeta{Name: metaName, Namespace: testNS},
		Spec:       api.KeyValueSpec{Bucket: bucket},
	}
}

func newKVWithAccount(metaName, bucket, account string) *api.KeyValue {
	kv := newKV(metaName, bucket)
	kv.Labels = map[string]string{NATSAccountLabel: account}
	return kv
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

// --- account-aware sibling-check (2026-05-26) ---------------------------

// testDefaultAccount is the configurable default account the tests wire into
// the validators, exercising the --default-account flag's normalization path.
const testDefaultAccount = "JS"

// TestDifferentAccounts_Normalization locks the resolve-then-compare table
// directly on the helper (the four cases from the fix's doc comment) plus
// the empty-default fallback to DefaultNATSAccount.
func TestDifferentAccounts_Normalization(t *testing.T) {
	lbl := func(v string) map[string]string { return map[string]string{NATSAccountLabel: v} }

	cases := []struct {
		name           string
		self, other    map[string]string
		defaultAccount string
		wantDifferent  bool
	}{
		{"unlabeled vs labeled-nats-qa", nil, lbl("nats-qa"), "JS", true},
		{"labeled-nats-qa vs unlabeled", lbl("nats-qa"), nil, "JS", true},
		{"unlabeled vs unlabeled", nil, nil, "JS", false},
		{"labeled-JS vs labeled-nats-qa", lbl("JS"), lbl("nats-qa"), "JS", true},
		{"labeled-JS vs labeled-JS", lbl("JS"), lbl("JS"), "JS", false},
		{"unlabeled vs label==default", nil, lbl("JS"), "JS", false},
		{"empty-string label treated as unlabeled", lbl(""), lbl("nats-qa"), "JS", true},
		{"empty default falls back to canonical", nil, lbl("nats-qa"), "", true},
		{"empty default: both unlabeled => same", nil, nil, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantDifferent, differentAccounts(tc.self, tc.other, tc.defaultAccount))
		})
	}
}

// Same spec.name but DIFFERENT NATS accounts => not a conflict.
// This is the bug from the failover-js--drp-orchestrator-rkpvx drill:
// activitylog-dev-2nd-east (account=JS) was rejected because
// activitylog-qa-2nd-east (account=nats-qa) also had spec.name="activitylog".
func TestStreamValidator_SameSpecName_DifferentAccounts_Allow(t *testing.T) {
	existing := newStreamWithAccount("activitylog-qa-2nd-east", "activitylog", "nats-qa")
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &StreamValidator{Client: c, DefaultAccount: testDefaultAccount}

	self := newStreamWithAccount("activitylog-dev-2nd-east", "activitylog", "JS")
	_, err := v.ValidateCreate(context.Background(), self)
	require.NoError(t, err, "different NATS accounts must not collide")
}

// Same spec.name AND same NATS account label => still a conflict.
func TestStreamValidator_SameSpecName_SameAccount_Reject(t *testing.T) {
	existing := newStreamWithAccount("orders-primary", "ORDERS", "JS")
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &StreamValidator{Client: c, DefaultAccount: testDefaultAccount}

	self := newStreamWithAccount("orders-mirror", "ORDERS", "JS")
	_, err := v.ValidateCreate(context.Background(), self)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ORDERS")
}

// Neither CR carries the label => both resolve to the default account =>
// still a conflict. This is the backward-compatible case: unlabeled-vs-
// unlabeled keeps the legacy conservative behavior.
func TestStreamValidator_SameSpecName_NoAccountLabel_Reject(t *testing.T) {
	existing := newStream("orders-primary", "ORDERS", nil)
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &StreamValidator{Client: c, DefaultAccount: testDefaultAccount}

	self := newStream("orders-mirror", "ORDERS", nil)
	_, err := v.ValidateCreate(context.Background(), self)
	require.Error(t, err, "two unlabeled CRs both resolve to the default account => conflict")
}

// THE FIX (dev-west 2026-05-30): an UNLABELED CR is the implicit DEFAULT
// account. An unlabeled JS stream (cob-orders-producer-dev) must NOT collide
// with its labeled nats-qa sibling (cob-orders-producer-qa) sharing the same
// spec.name. Pre-fix this rejected the QA CR and blocked the whole
// nacks-streams-sync-qa ArgoCD sync.
func TestStreamValidator_UnlabeledVsLabeledDifferent_Allow(t *testing.T) {
	// Live unlabeled JS sibling (implicit default account).
	existing := newStream("cob-orders-producer-dev", "cob-orders-producer", nil)
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &StreamValidator{Client: c, DefaultAccount: testDefaultAccount}

	// Labeled nats-qa CR being admitted.
	self := newStreamWithAccount("cob-orders-producer-qa", "cob-orders-producer", "nats-qa")
	_, err := v.ValidateCreate(context.Background(), self)
	require.NoError(t, err, "unlabeled (default account) vs labeled non-default must not collide")
}

// Symmetric to the above: admitting the unlabeled default-account CR while
// the labeled non-default sibling is already live must also be allowed
// (this is the deadlock half — each side validated against the other).
func TestStreamValidator_LabeledDifferentVsUnlabeled_Allow(t *testing.T) {
	existing := newStreamWithAccount("cob-orders-producer-qa", "cob-orders-producer", "nats-qa")
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &StreamValidator{Client: c, DefaultAccount: testDefaultAccount}

	self := newStream("cob-orders-producer-dev", "cob-orders-producer", nil) // unlabeled => "JS"
	_, err := v.ValidateCreate(context.Background(), self)
	require.NoError(t, err, "labeled non-default vs unlabeled (default account) must not collide")
}

// Unlabeled CR vs a labeled CR that resolves to the SAME default account
// (label value == the configured default) => still a conflict.
func TestStreamValidator_UnlabeledVsLabeledDefault_Reject(t *testing.T) {
	existing := newStreamWithAccount("orders-primary", "ORDERS", testDefaultAccount)
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &StreamValidator{Client: c, DefaultAccount: testDefaultAccount}

	self := newStream("orders-mirror", "ORDERS", nil) // unlabeled => default
	_, err := v.ValidateCreate(context.Background(), self)
	require.Error(t, err, "unlabeled and label==default both resolve to the default account => conflict")
}

// Mirror-relationship rules (rule 2/3) also respect the account filter.
func TestStreamValidator_MirrorAcrossDifferentAccounts_Allow(t *testing.T) {
	// sibling is a mirror of "ORDERS" but in account nats-qa.
	mirror := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders-mirror",
			Namespace: testNS,
			Labels:    map[string]string{NATSAccountLabel: "nats-qa"},
		},
		Spec: api.StreamSpec{Name: "ORDERS-MIRROR", Mirror: &api.StreamSource{Name: "ORDERS"}},
	}
	c := newFakeClient(t, newNamespace(testNS, ""), mirror)
	v := &StreamValidator{Client: c, DefaultAccount: testDefaultAccount}

	self := newStreamWithAccount("orders-primary", "ORDERS", "JS")
	_, err := v.ValidateCreate(context.Background(), self)
	require.NoError(t, err, "mirror across different accounts is not a conflict")
}

func TestKeyValueValidator_SameBucket_DifferentAccounts_Allow(t *testing.T) {
	existing := newKVWithAccount("config-qa", "CONFIG", "nats-qa")
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &KeyValueValidator{Client: c, DefaultAccount: testDefaultAccount}

	self := newKVWithAccount("config-js", "CONFIG", "JS")
	_, err := v.ValidateCreate(context.Background(), self)
	require.NoError(t, err)
}

func TestKeyValueValidator_SameBucket_SameAccount_Reject(t *testing.T) {
	existing := newKVWithAccount("config-primary", "CONFIG", "JS")
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &KeyValueValidator{Client: c, DefaultAccount: testDefaultAccount}

	self := newKVWithAccount("config-mirror", "CONFIG", "JS")
	_, err := v.ValidateCreate(context.Background(), self)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CONFIG")
}

// KV variant of THE FIX: unlabeled (default account) bucket vs labeled
// non-default bucket of the same name => not a conflict (consumer-offsets-qa
// incident half).
func TestKeyValueValidator_UnlabeledVsLabeledDifferent_Allow(t *testing.T) {
	existing := newKV("consumer-offsets-dev", "consumer-offsets") // unlabeled => default
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &KeyValueValidator{Client: c, DefaultAccount: testDefaultAccount}

	self := newKVWithAccount("consumer-offsets-qa", "consumer-offsets", "nats-qa")
	_, err := v.ValidateCreate(context.Background(), self)
	require.NoError(t, err, "unlabeled (default account) vs labeled non-default KV must not collide")
}

// KV unlabeled-vs-unlabeled still conflicts (default vs default).
func TestKeyValueValidator_SameBucket_NoLabel_Reject(t *testing.T) {
	existing := newKV("config-primary", "CONFIG")
	c := newFakeClient(t, newNamespace(testNS, ""), existing)
	v := &KeyValueValidator{Client: c, DefaultAccount: testDefaultAccount}

	self := newKV("config-mirror", "CONFIG")
	_, err := v.ValidateCreate(context.Background(), self)
	require.Error(t, err, "two unlabeled KV CRs both resolve to the default account => conflict")
}
