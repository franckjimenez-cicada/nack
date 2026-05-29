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

// drill_active_gate_test.go — coverage for the operator-only write gate.
//
// Decision matrix (from drillActiveOperatorGate's header):
//
//	| drill-active | scope-labeled | requester  | result   |
//	|--------------|---------------|------------|----------|
//	| no           | *             | *          | allowed  |  ← TestDrillActiveGate_NoDrill_AllowAnyone
//	| yes          | no            | *          | allowed  |  ← TestDrillActiveGate_DrillNoScope_AllowAnyone
//	| yes          | yes           | operator   | allowed  |  ← TestDrillActiveGate_DrillScopeOperator_Allow
//	| yes          | yes           | other      | REJECT   |  ← TestDrillActiveGate_DrillScopeOther_Reject
//
// Plus structural tests:
//
//   - The operator SA constant matches the canonical
//     `system:serviceaccount:nats:drp-operator` string (drift guard).
//   - The gate runs BEFORE the sibling-conflict check (a rejection from
//     the gate is the operator-only error, not the sibling-conflict one).
//   - The gate is wired into BOTH StreamValidator and KeyValueValidator
//     (same matrix, different CR kind).

package webhook

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

// scopedStream builds a scope-labeled Stream CR (the in-scope shape the gate
// protects). Account label is omitted so the legacy sibling-conflict path
// would fire conservatively if the gate didn't catch it first — useful for
// the "gate runs before sibling-conflict" test.
func scopedStream(metaName, specName string) *api.Stream {
	s := newStream(metaName, specName, nil)
	s.Labels = map[string]string{ScopeLabel: "true"}
	return s
}

// scopedKV is the KeyValue analogue of scopedStream.
func scopedKV(metaName, bucket string) *api.KeyValue {
	kv := newKV(metaName, bucket)
	kv.Labels = map[string]string{ScopeLabel: "true"}
	return kv
}

// ctxWithUser threads an admission.Request carrying the given username onto
// ctx. Mirrors what controller-runtime's admission webhook handler does for
// each incoming AdmissionReview in production — when validators read
// admission.RequestFromContext, they get back the Request the apiserver
// signed. Tests that exercise the gate's user check MUST use this helper;
// tests that don't (legacy sibling-conflict cases) intentionally pass a
// bare context.Background() so the gate short-circuits to allow via the
// "username == empty" unit-test path.
func ctxWithUser(username string) context.Context {
	return admission.NewContextWithRequest(context.Background(), admission.Request{
		AdmissionRequest: admissionv1Request(username),
	})
}

// admissionv1Request is split into its own helper so the type assertion
// chain stays readable. Returns the AdmissionRequest struct embedded in
// admission.Request (the v0.23 controller-runtime shape).
func admissionv1Request(username string) admissionv1.AdmissionRequest {
	return admissionv1.AdmissionRequest{
		UserInfo: authenticationv1.UserInfo{Username: username},
	}
}

// --- decision matrix ----------------------------------------------------

// TestDrillActiveGate_NoDrill_AllowAnyone — row 1 of the decision matrix.
// Outside a drill window the gate is a complete no-op, regardless of scope
// label or requester. Sanity: a kubectl-as-developer update on a scope-
// labeled CR succeeds when drill-active is absent.
func TestDrillActiveGate_NoDrill_AllowAnyone(t *testing.T) {
	// No drill annotation on the namespace.
	c := newFakeClient(t, newNamespace(testNS, ""))
	self := scopedStream("orders-primary", "ORDERS")

	for _, user := range []string{
		DRPOperatorServiceAccount,
		"system:serviceaccount:argocd:argocd-application-controller",
		"developer@example.com",
		"", // anonymous (RequestFromContext returns no Request)
	} {
		t.Run("user="+user, func(t *testing.T) {
			ctx := context.Background()
			if user != "" {
				ctx = ctxWithUser(user)
			}
			allowed, drillID, denyReason, err := drillActiveOperatorGate(ctx, c, self)
			require.NoError(t, err)
			require.True(t, allowed, "no-drill: gate must allow any user")
			require.Empty(t, drillID)
			require.Empty(t, denyReason)
		})
	}
}

// TestDrillActiveGate_DrillNoScope_AllowAnyone — row 2 of the decision
// matrix. A drill is in flight but THIS CR isn't scope-labeled, so the
// gate doesn't fire. Captures the explicit decision NOT to over-reach to
// arbitrary CRs that happen to live in a drill-active namespace.
func TestDrillActiveGate_DrillNoScope_AllowAnyone(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-29-abc"))
	// No ScopeLabel set on the CR.
	self := newStream("unrelated-stream", "UNRELATED", nil)

	ctx := ctxWithUser("system:serviceaccount:argocd:argocd-application-controller")
	allowed, drillID, denyReason, err := drillActiveOperatorGate(ctx, c, self)
	require.NoError(t, err)
	require.True(t, allowed, "drill-active but CR not scope-labeled: gate must allow")
	require.Equal(t, "drill-2026-05-29-abc", drillID)
	require.Empty(t, denyReason)
}

// TestDrillActiveGate_DrillScopeOperator_Allow — row 3 of the decision
// matrix. The happy path: drill in flight, CR is scope-labeled, requester
// IS the drp-operator. The destructive promote cycle must proceed.
func TestDrillActiveGate_DrillScopeOperator_Allow(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-29-abc"))
	self := scopedStream("orders-primary", "ORDERS")
	ctx := ctxWithUser(DRPOperatorServiceAccount)

	allowed, drillID, denyReason, err := drillActiveOperatorGate(ctx, c, self)
	require.NoError(t, err)
	require.True(t, allowed, "drill+scope+operator: gate must allow")
	require.Equal(t, "drill-2026-05-29-abc", drillID)
	require.Empty(t, denyReason)
}

// TestDrillActiveGate_DrillScopeOther_Reject — row 4 of the decision matrix.
// The load-bearing case: ArgoCD's application-controller tries to UPDATE a
// scope-labeled CR while the operator's destructive promote is in flight.
// The gate rejects with a clear remediation hint.
func TestDrillActiveGate_DrillScopeOther_Reject(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-29-abc"))
	self := scopedStream("orders-primary", "ORDERS")

	for _, user := range []string{
		"system:serviceaccount:argocd:argocd-application-controller",
		"system:serviceaccount:argocd:argocd-server",
		"developer@example.com",
		"system:serviceaccount:kube-system:default", // some random SA
	} {
		t.Run("user="+user, func(t *testing.T) {
			ctx := ctxWithUser(user)
			allowed, drillID, denyReason, err := drillActiveOperatorGate(ctx, c, self)
			require.NoError(t, err)
			require.False(t, allowed, "drill+scope+non-operator: gate must REJECT")
			require.Equal(t, "drill-2026-05-29-abc", drillID)
			require.Contains(t, denyReason, "user=")
			require.Contains(t, denyReason, user)
			require.Contains(t, denyReason, "drill-active=")
		})
	}
}

// --- structural / drift guards -----------------------------------------

// TestDRPOperatorServiceAccount_CanonicalValue pins the operator SA path.
// If drp-operator's chart ever renames the SA or moves to a different
// namespace, this constant must update in lock-step. The test catches
// the drift at build time so the webhook doesn't silently reject the
// operator itself in production.
func TestDRPOperatorServiceAccount_CanonicalValue(t *testing.T) {
	const want = "system:serviceaccount:nats:drp-operator"
	require.Equal(t, want, DRPOperatorServiceAccount,
		"drp-operator chart pins ns=nats sa=drp-operator; webhook constant must match")
}

// TestIsScopeLabeled covers the in-memory label test the gate uses to
// decide whether a CR is in-scope. Empty value is treated as ABSENT (the
// label must be present AND have a non-empty value to count). nil label
// map is the no-op base case.
func TestIsScopeLabeled(t *testing.T) {
	cases := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"nil-map", nil, false},
		{"empty-map", map[string]string{}, false},
		{"label-absent", map[string]string{"other": "value"}, false},
		{"label-present-true", map[string]string{ScopeLabel: "true"}, true},
		{"label-present-other-value", map[string]string{ScopeLabel: "yes"}, true},
		{"label-present-empty-value", map[string]string{ScopeLabel: ""}, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isScopeLabeled(tc.labels))
		})
	}
}

// TestRequesterUsername_NoRequestInContext — when controller-runtime
// hasn't threaded an admission.Request onto ctx (unit-test path), the
// helper returns "" so the gate falls through to allow. This is the
// backward-compat tail that lets pre-existing legacy tests
// (sibling_test.go, written before the gate) keep passing without
// rewriting every fixture to seed an admission.Request.
func TestRequesterUsername_NoRequestInContext(t *testing.T) {
	require.Empty(t, requesterUsername(context.Background()))
}

// TestRequesterUsername_RequestPresent reads the username back through
// the gate's helper to confirm the context plumbing matches what
// controller-runtime does in production.
func TestRequesterUsername_RequestPresent(t *testing.T) {
	ctx := ctxWithUser("test-user")
	require.Equal(t, "test-user", requesterUsername(ctx))
}

// --- validator integration ---------------------------------------------

// TestStreamValidator_DrillActiveOperatorGate_RejectsArgoCD pins the
// validator wiring: when ArgoCD's application-controller tries to update
// a scope-labeled Stream during a drill, the validator returns an error
// surfacing the operator-only remediation hint (NOT the sibling-conflict
// hint). Cross-checks that the gate runs BEFORE the sibling-conflict
// path.
func TestStreamValidator_DrillActiveOperatorGate_RejectsArgoCD(t *testing.T) {
	// A second CR exists with the SAME spec.name — so if the gate didn't
	// fire first, the sibling-conflict check would catch the request and
	// produce its OWN error. The test verifies the gate's error wins.
	existing := newStream("orders-primary", "ORDERS", nil)
	c := newFakeClient(t,
		newNamespace(testNS, "drill-2026-05-29-abc"),
		existing,
	)
	v := &StreamValidator{Client: c}

	self := scopedStream("orders-mirror", "ORDERS")
	argoUser := "system:serviceaccount:argocd:argocd-application-controller"
	ctx := ctxWithUser(argoUser)
	_, err := v.ValidateUpdate(ctx, existing, self)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drill-active operator-only gate")
	require.Contains(t, err.Error(), argoUser,
		"rejection must name the offending requester for triage")
	require.Contains(t, err.Error(), DrillActiveAnnotation,
		"remediation hint must name the annotation operators can remove")
	require.NotContains(t, err.Error(), "sibling-conflict webhook",
		"operator-only gate must fire BEFORE sibling-conflict check")
}

// TestStreamValidator_DrillActiveOperatorGate_AllowsOperator confirms the
// operator's UPDATE on the same scope-labeled Stream passes through (with
// the sibling-conflict path then allowing it because drill-active is set
// — the existing v1 sibling-allow-during-drill warning fires).
func TestStreamValidator_DrillActiveOperatorGate_AllowsOperator(t *testing.T) {
	existing := newStream("orders-primary", "ORDERS", nil)
	c := newFakeClient(t,
		newNamespace(testNS, "drill-2026-05-29-abc"),
		existing,
	)
	v := &StreamValidator{Client: c}

	self := scopedStream("orders-mirror", "ORDERS")
	self.Spec.Mirror = &api.StreamSource{Name: "ORDERS"}
	ctx := ctxWithUser(DRPOperatorServiceAccount)

	warnings, err := v.ValidateUpdate(ctx, existing, self)
	require.NoError(t, err, "operator SA must be allowed through both gate and sibling-conflict path")
	// The sibling-conflict legacy "allowed during drill" warning still
	// fires because the existing primary spec.name=ORDERS conflicts with
	// our self.spec.name=ORDERS. The gate doesn't suppress warnings, only
	// hard rejections.
	require.NotEmpty(t, warnings)
}

// TestKeyValueValidator_DrillActiveOperatorGate_RejectsArgoCD is the
// KeyValue analogue of the Stream rejection test. Same shape, different
// validator wiring — pins the gate runs on KV CRs too.
func TestKeyValueValidator_DrillActiveOperatorGate_RejectsArgoCD(t *testing.T) {
	existing := newKV("config-primary", "CONFIG")
	c := newFakeClient(t,
		newNamespace(testNS, "drill-2026-05-29-abc"),
		existing,
	)
	v := &KeyValueValidator{Client: c}

	self := scopedKV("config-mirror", "CONFIG")
	ctx := ctxWithUser("system:serviceaccount:argocd:argocd-application-controller")
	_, err := v.ValidateUpdate(ctx, existing, self)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drill-active operator-only gate")
	require.NotContains(t, err.Error(), "sibling-conflict webhook")
}

// TestKeyValueValidator_DrillActiveOperatorGate_AllowsOperator is the KV
// analogue of the Stream "operator allowed" test.
func TestKeyValueValidator_DrillActiveOperatorGate_AllowsOperator(t *testing.T) {
	existing := newKV("config-primary", "CONFIG")
	c := newFakeClient(t,
		newNamespace(testNS, "drill-2026-05-29-abc"),
		existing,
	)
	v := &KeyValueValidator{Client: c}

	self := scopedKV("config-mirror", "CONFIG")
	ctx := ctxWithUser(DRPOperatorServiceAccount)

	warnings, err := v.ValidateUpdate(ctx, existing, self)
	require.NoError(t, err)
	require.NotEmpty(t, warnings, "sibling-conflict legacy allow-during-drill warning should still fire")
}

// TestStreamValidator_NoDrill_NoUserCheck — backward-compat anchor.
// Existing sibling-conflict tests (written before the gate) all pass
// bare context.Background() and expect either a reject (legacy
// sibling-conflict) or a pass-through warning. The gate must NEVER fire
// outside a drill window, so those tests keep working without
// modification. This test makes the contract explicit so a future change
// to the gate that broadens its scope (e.g. "reject non-operator writes
// always") trips here loudly.
func TestStreamValidator_NoDrill_NoUserCheck(t *testing.T) {
	existing := newStream("orders-primary", "ORDERS", nil)
	c := newFakeClient(t, newNamespace(testNS, ""), existing) // no drill
	v := &StreamValidator{Client: c}

	self := scopedStream("orders-mirror", "ORDERS")
	// Bare context — no admission.Request — emulates the legacy test
	// shape. The gate must not fire; the request reaches sibling-conflict
	// and is rejected with the LEGACY message (not the gate's).
	_, err := v.ValidateUpdate(context.Background(), existing, self)
	require.Error(t, err)
	require.Contains(t, err.Error(), "sibling-conflict webhook")
	require.NotContains(t, err.Error(), "drill-active operator-only gate",
		"gate must NOT fire outside a drill window")
}

// --- sanity wires (defensive imports) ----------------------------------

// keep the imports anchored — same rationale as the gate file's _ var
// (catches a refactor that drops a needed import).
var _ = metav1.ObjectMeta{}
var _ = strings.Contains
