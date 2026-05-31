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
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
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

// callGate is a thin wrapper that lets the matrix-style tests below stay
// compact while still threading the configurable operator-SA through. Most
// tests pin the canonical default; the configured-SA test below overrides.
func callGate(ctx context.Context, c ctrlclient.Client, obj objectLabels) (bool, string, string, error) {
	return drillActiveOperatorGate(ctx, c, obj, DefaultDRPOperatorServiceAccount, DefaultControllerServiceAccount)
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
			allowed, drillID, denyReason, err := callGate(ctx, c, self)
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
	allowed, drillID, denyReason, err := callGate(ctx, c, self)
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

	allowed, drillID, denyReason, err := callGate(ctx, c, self)
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
			allowed, drillID, denyReason, err := callGate(ctx, c, self)
			require.NoError(t, err)
			require.False(t, allowed, "drill+scope+non-operator: gate must REJECT")
			require.Equal(t, "drill-2026-05-29-abc", drillID)
			require.Contains(t, denyReason, "user=")
			require.Contains(t, denyReason, user)
			require.Contains(t, denyReason, "drill-active=")
		})
	}
}

// TestDrillActiveGate_LiteralFalseAnnotation pins the gate's reading of the
// drill-active annotation: today's contract treats ANY non-empty value as
// "drill in flight" (the value is the drillID for audit). The literal
// string "false" is therefore NOT an off-switch — it's a value, and the
// gate fires. Operators that want to disable the gate must REMOVE the
// annotation, not set it to "false". This test pins that semantics so a
// future "false means false" change is intentional and tripwired here.
//
// Rationale for the pin: leaving "false"/"0"/"no" handling unspecified
// would let two opposite mental models coexist (annotation-as-flag vs
// annotation-as-drillID). We pick annotation-as-drillID because the
// canonical drp-operator code path always writes the drillID, never a
// boolean. The webhook gate must match.
func TestDrillActiveGate_LiteralFalseAnnotation(t *testing.T) {
	// Annotation present with literal "false" — still treated as drill
	// in flight per the annotation-as-drillID contract.
	c := newFakeClient(t, newNamespace(testNS, "false"))
	self := scopedStream("orders-primary", "ORDERS")

	t.Run("operator-allowed-as-usual", func(t *testing.T) {
		ctx := ctxWithUser(DRPOperatorServiceAccount)
		allowed, drillID, _, err := callGate(ctx, c, self)
		require.NoError(t, err)
		require.True(t, allowed, "operator SA passes regardless of drill annotation value")
		require.Equal(t, "false", drillID, "annotation value is surfaced verbatim as drillID")
	})

	t.Run("non-operator-rejected-because-annotation-is-non-empty", func(t *testing.T) {
		ctx := ctxWithUser("system:serviceaccount:argocd:argocd-application-controller")
		allowed, _, denyReason, err := callGate(ctx, c, self)
		require.NoError(t, err)
		require.False(t, allowed,
			"literal 'false' is a value, not an off-switch — gate must fire (remove annotation to disable)")
		require.Contains(t, denyReason, "drill-active=\"false\"",
			"denyReason carries the literal annotation value for triage")
	})
}

// TestDrillActiveGate_ConfiguredOperatorSA exercises the flag-driven
// override path: a prod-style deployment may run drp-operator under a
// different namespace/SA pair, so the gate must compare against the
// configured value, NOT the hardcoded dev/stg default.
//
// Two assertions in one test (a deliberate pairing):
//
//  1. The configured prod-style SA is accepted by the gate.
//  2. The dev/stg default SA is REJECTED when the operator binary was
//     started with the prod override (configuration drift between the
//     operator's actual identity and the webhook's expectation is a
//     hard-failure, not a silent allow).
func TestDrillActiveGate_ConfiguredOperatorSA(t *testing.T) {
	const prodOperatorSA = "system:serviceaccount:nats-prod:drp-operator-prod"

	c := newFakeClient(t, newNamespace(testNS, "drill-prod-2026-06-01"))
	self := scopedStream("orders-primary", "ORDERS")

	t.Run("configured-SA-allowed", func(t *testing.T) {
		ctx := ctxWithUser(prodOperatorSA)
		allowed, _, _, err := drillActiveOperatorGate(ctx, c, self, prodOperatorSA, DefaultControllerServiceAccount)
		require.NoError(t, err)
		require.True(t, allowed, "configured operator SA must be allowed")
	})

	t.Run("dev-default-SA-rejected-under-prod-config", func(t *testing.T) {
		ctx := ctxWithUser(DefaultDRPOperatorServiceAccount)
		allowed, _, denyReason, err := drillActiveOperatorGate(ctx, c, self, prodOperatorSA, DefaultControllerServiceAccount)
		require.NoError(t, err)
		require.False(t, allowed,
			"dev-default SA must NOT be silently accepted when prod SA is configured")
		require.Contains(t, denyReason, DefaultDRPOperatorServiceAccount)
	})

	t.Run("empty-operatorSA-falls-back-to-default", func(t *testing.T) {
		// Belt-and-suspenders: passing "" through the gate falls back
		// to DefaultDRPOperatorServiceAccount so a missing flag wire-up
		// doesn't open the gate to everyone.
		ctx := ctxWithUser(DefaultDRPOperatorServiceAccount)
		allowed, _, _, err := drillActiveOperatorGate(ctx, c, self, "", "")
		require.NoError(t, err)
		require.True(t, allowed, "empty configured SA must fall back to the default — not allow everyone")
	})
}

// --- controller-self SA exemption (live deadlock 2026-05-31) ------------
//
// The gate originally exempted ONLY the drp-operator SA. nack's own
// controller (`system:serviceaccount:nats:jetstream-controller`) was
// rejected when it issued the finalizer-removal UPDATE while deleting a
// scope-labeled CR on the operator's behalf — the CR stuck in Terminating,
// the operator's wait-gone timed out, the drill failed. These tests pin the
// regression: the controller-self SA must ALWAYS be allowed mid-drill, while
// every other non-operator SA stays rejected.

// TestDrillActiveGate_DrillScopeControllerSelf_Allow — the regression for
// the 2026-05-31 deadlock. drill in flight + scope-labeled CR + requester is
// nack's own controller SA → ALLOWED (so finalizer removal / reconcile can
// proceed and the CR can leave Terminating).
func TestDrillActiveGate_DrillScopeControllerSelf_Allow(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-31-xyz"))
	self := scopedStream("orders-primary", "ORDERS")
	ctx := ctxWithUser(DefaultControllerServiceAccount)

	allowed, drillID, denyReason, err := callGate(ctx, c, self)
	require.NoError(t, err)
	require.True(t, allowed,
		"drill+scope+controller-self: gate must ALLOW (nack must manage its own CRs/finalizers mid-drill)")
	require.Equal(t, "drill-2026-05-31-xyz", drillID)
	require.Empty(t, denyReason)
}

// TestDrillActiveGate_DrillScopeOther_RejectIncludesArgo confirms the gate
// still REJECTS everyone else even after the controller-self exemption is
// added — only the operator SA and the controller-self SA pass. ArgoCD and a
// human stay blocked.
func TestDrillActiveGate_DrillScopeOther_RejectIncludesArgo(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-31-xyz"))
	self := scopedStream("orders-primary", "ORDERS")

	for _, user := range []string{
		"system:serviceaccount:argocd:argocd-application-controller",
		"developer@example.com",
		// a controller SA in the WRONG namespace must NOT be exempt — the
		// exemption is the exact SA string, not a name-only match.
		"system:serviceaccount:other-ns:jetstream-controller",
	} {
		t.Run("user="+user, func(t *testing.T) {
			ctx := ctxWithUser(user)
			allowed, _, denyReason, err := callGate(ctx, c, self)
			require.NoError(t, err)
			require.False(t, allowed, "non-operator, non-controller SA must be REJECTED")
			require.Contains(t, denyReason, user)
		})
	}
}

// TestDrillActiveGate_ConfiguredSelfSA exercises the configurable override
// of the controller-self SA: a cluster that renames the controller SA passes
// the new value via --self-service-account, and the gate must compare
// against THAT, not the hardcoded dev/stg default.
func TestDrillActiveGate_ConfiguredSelfSA(t *testing.T) {
	const customSelfSA = "system:serviceaccount:nats-east:jetstream-controller-east"

	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-31-xyz"))
	self := scopedStream("orders-primary", "ORDERS")

	t.Run("configured-self-SA-allowed", func(t *testing.T) {
		ctx := ctxWithUser(customSelfSA)
		allowed, _, _, err := drillActiveOperatorGate(ctx, c, self, DefaultDRPOperatorServiceAccount, customSelfSA)
		require.NoError(t, err)
		require.True(t, allowed, "configured controller-self SA must be allowed")
	})

	t.Run("dev-default-self-SA-rejected-under-custom-config", func(t *testing.T) {
		// When a non-default self SA is configured, the dev default must NOT
		// be silently allowed (no name-only matching).
		ctx := ctxWithUser(DefaultControllerServiceAccount)
		allowed, _, denyReason, err := drillActiveOperatorGate(ctx, c, self, DefaultDRPOperatorServiceAccount, customSelfSA)
		require.NoError(t, err)
		require.False(t, allowed,
			"dev-default controller SA must NOT be accepted when a custom self SA is configured")
		require.Contains(t, denyReason, DefaultControllerServiceAccount)
	})

	t.Run("empty-selfSA-falls-back-to-default", func(t *testing.T) {
		ctx := ctxWithUser(DefaultControllerServiceAccount)
		allowed, _, _, err := drillActiveOperatorGate(ctx, c, self, DefaultDRPOperatorServiceAccount, "")
		require.NoError(t, err)
		require.True(t, allowed,
			"empty self SA must fall back to DefaultControllerServiceAccount — not reject the controller")
	})
}

// TestDefaultControllerServiceAccount_CanonicalValue pins nack's self SA to
// the value it actually runs as in dev-west: SA name is the chart default
// `jetstream-controller` (children/nacks jsc.serviceAccountName default),
// namespace is `nats` (nacks ArgoCD Application destination namespace). If
// the chart renames the SA cluster-wide, update this constant in lock-step
// OR set --self-service-account.
func TestDefaultControllerServiceAccount_CanonicalValue(t *testing.T) {
	const want = "system:serviceaccount:nats:jetstream-controller"
	require.Equal(t, want, DefaultControllerServiceAccount,
		"nack chart deploys SA=jetstream-controller in ns=nats; self-SA constant must match")
}

// TestResolveControllerServiceAccount covers the override + POD_NAMESPACE
// auto-detection precedence.
func TestResolveControllerServiceAccount(t *testing.T) {
	envWith := func(ns string) func(string) string {
		return func(k string) string {
			if k == PodNamespaceEnv {
				return ns
			}
			return ""
		}
	}

	cases := []struct {
		name     string
		override string
		getenv   func(string) string
		want     string
	}{
		{
			name:     "override-wins",
			override: "system:serviceaccount:custom:custom-sa",
			getenv:   envWith("ignored-ns"),
			want:     "system:serviceaccount:custom:custom-sa",
		},
		{
			name:     "auto-detect-namespace-from-env",
			override: "",
			getenv:   envWith("nats-east"),
			want:     "system:serviceaccount:nats-east:jetstream-controller",
		},
		{
			name:     "no-override-no-env-falls-back-to-default",
			override: "",
			getenv:   envWith(""),
			want:     DefaultControllerServiceAccount,
		},
		{
			name:     "nil-getenv-falls-back-to-default",
			override: "",
			getenv:   nil,
			want:     DefaultControllerServiceAccount,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ResolveControllerServiceAccount(tc.override, tc.getenv))
		})
	}
}

// --- ValidateDelete matrix ---------------------------------------------
//
// ArgoCD prune on a scope-labeled CR mid-drill MUST be rejected. The 4
// rows below mirror the CREATE/UPDATE decision matrix, scoped to DELETE.

// TestDrillActiveGate_NoDrill_DeleteAllowsAnyone — row 1 of the matrix
// applied to DELETE. Outside a drill, any deleter passes (kubectl, Argo,
// operator).
func TestDrillActiveGate_NoDrill_DeleteAllowsAnyone(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, ""))
	v := &StreamValidator{Client: c}
	self := scopedStream("orders-primary", "ORDERS")
	for _, user := range []string{
		DRPOperatorServiceAccount,
		"system:serviceaccount:argocd:argocd-application-controller",
		"developer@example.com",
	} {
		t.Run("user="+user, func(t *testing.T) {
			ctx := ctxWithUser(user)
			_, err := v.ValidateDelete(ctx, self)
			require.NoError(t, err, "no-drill DELETE must be allowed for any user")
		})
	}
}

// TestDrillActiveGate_DrillNoScope_DeleteAllowsAnyone — row 2: drill is
// active but the CR isn't scope-labeled, so the gate is a no-op and any
// deleter passes through.
func TestDrillActiveGate_DrillNoScope_DeleteAllowsAnyone(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-29-abc"))
	v := &StreamValidator{Client: c}
	// Unscoped Stream.
	self := newStream("unrelated-stream", "UNRELATED", nil)

	ctx := ctxWithUser("system:serviceaccount:argocd:argocd-application-controller")
	_, err := v.ValidateDelete(ctx, self)
	require.NoError(t, err, "drill-active but unscoped CR: DELETE must be allowed for non-operator")
}

// TestDrillActiveGate_DrillScopeOperator_DeleteAllows — row 3: the happy
// path that PromotingDestination / DemotingSource subphases rely on. The
// operator's delete (which is half of its delete+create promote cycle)
// must always succeed.
func TestDrillActiveGate_DrillScopeOperator_DeleteAllows(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-29-abc"))
	v := &StreamValidator{Client: c}
	self := scopedStream("orders-primary", "ORDERS")

	ctx := ctxWithUser(DRPOperatorServiceAccount)
	_, err := v.ValidateDelete(ctx, self)
	require.NoError(t, err, "operator SA DELETE on scope-labeled CR must be allowed (promote delete+create needs this)")
}

// TestDrillActiveGate_DrillScopeOther_DeleteRejects — row 4: ArgoCD's
// prune of a scope-labeled CR mid-drill is the exact failure mode the
// original 2026-05-29 incident triggered ("3 NotFound — chart re-deleted
// the CR after the operator's delete"). The DELETE gate closes this hole.
func TestDrillActiveGate_DrillScopeOther_DeleteRejects(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-29-abc"))
	v := &StreamValidator{Client: c}
	self := scopedStream("orders-primary", "ORDERS")

	argoUser := "system:serviceaccount:argocd:argocd-application-controller"
	ctx := ctxWithUser(argoUser)
	_, err := v.ValidateDelete(ctx, self)
	require.Error(t, err, "ArgoCD prune on scope-labeled CR mid-drill must be rejected")
	require.Contains(t, err.Error(), "drill-active operator-only gate")
	require.Contains(t, err.Error(), argoUser)
}

// TestKeyValueValidator_DrillScopeOther_DeleteRejects — same shape for
// KeyValue CRs. The KV chart also runs through ArgoCD; same rejection.
func TestKeyValueValidator_DrillScopeOther_DeleteRejects(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-29-abc"))
	v := &KeyValueValidator{Client: c}
	self := scopedKV("config-primary", "CONFIG")

	ctx := ctxWithUser("system:serviceaccount:argocd:argocd-application-controller")
	_, err := v.ValidateDelete(ctx, self)
	require.Error(t, err, "ArgoCD prune on scope-labeled KV mid-drill must be rejected")
	require.Contains(t, err.Error(), "drill-active operator-only gate")
}

// TestKeyValueValidator_DrillScopeOperator_DeleteAllows — KV operator
// happy path. Mirrors the Stream test.
func TestKeyValueValidator_DrillScopeOperator_DeleteAllows(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-29-abc"))
	v := &KeyValueValidator{Client: c}
	self := scopedKV("config-primary", "CONFIG")

	ctx := ctxWithUser(DRPOperatorServiceAccount)
	_, err := v.ValidateDelete(ctx, self)
	require.NoError(t, err)
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

// TestStreamValidator_DrillActiveOperatorGate_AllowsControllerSelf is the
// validator-level regression for the 2026-05-31 deadlock: nack's own
// controller UPDATE (finalizer strip) on a scope-labeled Stream during a
// drill must pass the gate. The validator's ControllerSelfSA defaults to
// DefaultControllerServiceAccount when unset, so we set it explicitly here
// to the canonical value (mirrors the wired-up production default).
func TestStreamValidator_DrillActiveOperatorGate_AllowsControllerSelf(t *testing.T) {
	existing := newStream("orders-primary", "ORDERS", nil)
	c := newFakeClient(t,
		newNamespace(testNS, "drill-2026-05-31-xyz"),
		existing,
	)
	v := &StreamValidator{Client: c, ControllerSelfSA: DefaultControllerServiceAccount}

	self := scopedStream("orders-mirror", "ORDERS")
	self.Spec.Mirror = &api.StreamSource{Name: "ORDERS"}
	ctx := ctxWithUser(DefaultControllerServiceAccount)

	warnings, err := v.ValidateUpdate(ctx, existing, self)
	require.NoError(t, err,
		"controller-self SA must pass the gate so nack can manage its own CRs/finalizers mid-drill")
	// sibling-conflict legacy allow-during-drill warning still fires.
	require.NotEmpty(t, warnings)
}

// TestStreamValidator_DrillScopeControllerSelf_DeleteAllows pins the DELETE
// path: when nack reconciles the operator's CR deletion, the finalizer-strip
// happens via an UPDATE, but the controller may also issue DELETE-adjacent
// calls; the gate must not block the controller SA on DELETE either.
func TestStreamValidator_DrillScopeControllerSelf_DeleteAllows(t *testing.T) {
	c := newFakeClient(t, newNamespace(testNS, "drill-2026-05-31-xyz"))
	v := &StreamValidator{Client: c, ControllerSelfSA: DefaultControllerServiceAccount}
	self := scopedStream("orders-primary", "ORDERS")

	ctx := ctxWithUser(DefaultControllerServiceAccount)
	_, err := v.ValidateDelete(ctx, self)
	require.NoError(t, err, "controller-self SA DELETE on scope-labeled CR must be allowed mid-drill")
}

// TestKeyValueValidator_DrillActiveOperatorGate_AllowsControllerSelf is the
// KeyValue analogue of the Stream controller-self allow test.
func TestKeyValueValidator_DrillActiveOperatorGate_AllowsControllerSelf(t *testing.T) {
	existing := newKV("config-primary", "CONFIG")
	c := newFakeClient(t,
		newNamespace(testNS, "drill-2026-05-31-xyz"),
		existing,
	)
	v := &KeyValueValidator{Client: c, ControllerSelfSA: DefaultControllerServiceAccount}

	self := scopedKV("config-mirror", "CONFIG")
	ctx := ctxWithUser(DefaultControllerServiceAccount)

	warnings, err := v.ValidateUpdate(ctx, existing, self)
	require.NoError(t, err, "controller-self SA must pass the gate on KV CRs too")
	require.NotEmpty(t, warnings)
}

// --- sanity wires (defensive imports) ----------------------------------

// keep the imports anchored — same rationale as the gate file's _ var
// (catches a refactor that drops a needed import).
var _ = metav1.ObjectMeta{}
var _ = strings.Contains
