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

// drill_active_gate.go — operator-only write gate during a coordinated drill.
//
// Live failure 2026-05-29 (E→W flip): PromotingDestination reported "7/17
// resources failed to promote" because `nacks-streams-sync` ArgoCD App stayed
// `automated.selfHeal=true` during the NATS step and raced the operator's
// promote delete+create cycle on scope-labeled Stream / KeyValue CRs.
// Outcomes seen on the 7 affected streams:
//
//   - 3 NotFound  — chart re-deleted the CR after the operator's delete + before
//                   the operator's recreate landed.
//   - 3 hybrid    — chart re-stamped spec.mirror back onto a CR the operator
//                   had already promoted (mirror=nil + subjects=[...]); CR
//                   ends up with BOTH mirror and subjects, an invalid shape.
//   - 1 wait-gone — chart kept resurrecting the CR; the operator's wait-for-
//                   gone polling burned its budget waiting for the delete
//                   to settle.
//
// The fix is admission-time, not operator-side. While a drill is in flight on
// a namespace (`drp.cicada.io/drill-active=<drillID>` set on the Namespace
// object by drp-operator's PromotingDestination subphase), this gate REJECTS
// any CREATE/UPDATE on a scope-labeled (`drp.cicada.io/nats-failover-scope=
// true`) Stream or KeyValue CR if the AdmissionReview's requester is NOT
// the drp-operator's ServiceAccount.
//
// What this means in practice:
//
//   - drp-operator's promote delete+create cycle proceeds unimpeded (its
//     requests come from `system:serviceaccount:nats:drp-operator`).
//   - ArgoCD's selfHeal-driven UPDATE to re-stamp `spec.mirror` back onto a
//     just-promoted CR is rejected. ArgoCD marks the App OutOfSync but the
//     CR stays in the operator-promoted state. When drill-active clears at
//     the end of the drill (drp-operator's deferred restore), ArgoCD's next
//     sync converges to the new chart state.
//   - Local-dev / kubectl-as-developer pokes during a drill are rejected
//     with a clear remediation hint.
//   - Outside a drill, the gate is a no-op — admission falls through to the
//     existing sibling-conflict check.
//
// Why operator-only (not just "non-ArgoCD-only"): the gate is positive,
// not negative. We don't enumerate the set of bad actors (ArgoCD, manual
// kubectl, dashboard auto-reconciler, etc.); we name the SINGLE writer that
// is allowed during a drill and reject everyone else. This is the minimum-
// privilege shape: anything new that tries to write a scope-labeled CR
// during a drill (CI bots, helm operators, GitOps tooling) is rejected by
// default. The operator's username is the ONE thing we trust.
//
// Coordination with the existing sibling-conflict path: the operator-only
// gate runs FIRST. If the gate rejects, validation returns immediately —
// the sibling-conflict check is bypassed because the request never reaches
// the controller anyway. If the gate is inactive (no drill / not scope-
// labeled / requester IS the operator), validation falls through to the
// existing sibling-conflict check unchanged. The two paths are
// complementary, not overlapping.

package webhook

import (
	"context"
	"fmt"

	authenticationv1 "k8s.io/api/authentication/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// ScopeLabel is the K8s label that marks a Stream / KeyValue CR as in-scope
// for a DRP drill. drp-operator's promote subphases mutate these CRs
// destructively (delete + recreate), and ArgoCD's selfHeal-driven UPDATEs
// would race that cycle. The operator-only gate triggers ONLY for CRs
// carrying this label — non-scope CRs (e.g. an unrelated Stream the chart
// happens to manage in the same namespace) are not affected.
//
// Canonical value is "true"; we don't check the value (any non-empty value
// is treated as in-scope) to match the operator side's tolerance for
// future label-value evolution.
const ScopeLabel = "drp.cicada.io/nats-failover-scope"

// DRPOperatorServiceAccount is the canonical Kubernetes ServiceAccount the
// drp-operator runs as. Format matches admission.Request.UserInfo.Username
// for in-cluster ServiceAccount requests: `system:serviceaccount:<ns>:<sa>`.
//
// Values:
//
//   - <ns>: `nats` — the operator's deployment namespace, from
//     drp-operator's chart/values.yaml `namespace: nats`.
//   - <sa>: `drp-operator` — the SA name, from drp-operator's
//     chart/values.yaml `serviceAccount.name: drp-operator`.
//
// Confirmed by inspecting drp-operator@770c84a (main) chart/values.yaml.
// If the operator's deployment namespace or SA name ever change, this
// constant must be updated in lock-step or the gate will reject the
// operator itself, breaking every drill cluster-wide. The webhook unit
// tests pin the value so a drift is caught at build time.
const DRPOperatorServiceAccount = "system:serviceaccount:nats:drp-operator"

// operatorOnlyRemediationHint is the message tail returned alongside every
// operator-only rejection. The first sentence names the rule; the second
// gives the operator-friendly recovery action (wait for drill-active to
// clear); the third gives the test-only escape hatch.
const operatorOnlyRemediationHint = "Stream/KeyValue updates on scope-labeled CRs are blocked during a DRP drill " +
	"(namespace annotation '" + DrillActiveAnnotation + "' is set). " +
	"Only the drp-operator ServiceAccount may mutate these CRs while a drill is in flight. " +
	"The request will succeed automatically once the drill completes and clears the annotation. " +
	"If you're testing locally without a real drill, remove the '" + DrillActiveAnnotation + "' annotation from the namespace."

// objectLabels is the minimal accessor surface the gate needs: read the
// CR's labels (to test for scope) and its namespace (to look up
// drill-active). Both Stream and KeyValue satisfy this via embedded
// ObjectMeta.
type objectLabels interface {
	GetLabels() map[string]string
	GetNamespace() string
}

// drillActiveOperatorGate is the operator-only write-gate decision.
//
// Returns (allowed, drillID, denyReason, err):
//
//   - allowed=true means the gate is inactive (no drill / not scope-
//     labeled) OR the request comes from the operator. Validation
//     continues with the existing sibling-conflict check.
//   - allowed=false means the gate fired and the request must be REJECTED
//     with operatorOnlyRemediationHint. denyReason summarizes which input
//     tripped the gate ("user=<X>", "drill-active=<drillID>").
//   - err signals a webhook infrastructure failure (failed to read the
//     Namespace object). Per failurePolicy=Fail, the apiserver rejects.
//
// Decision matrix (in order — first match wins):
//
//	| drill-active | scope-labeled | requester  | result   |
//	|--------------|---------------|------------|----------|
//	| no           | *             | *          | allowed  |
//	| yes          | no            | *          | allowed  |
//	| yes          | yes           | operator   | allowed  |
//	| yes          | yes           | other      | REJECT   |
//
// `requester` comes from the AdmissionReview's `request.userInfo.username`
// surfaced via admission.RequestFromContext (controller-runtime threads it
// onto the validator's ctx). When the context lacks an admission.Request
// (unit-test path), we treat the requester as the operator — tests that
// want to exercise the rejection path set up the request explicitly via
// admission.NewContextWithRequest.
func drillActiveOperatorGate(ctx context.Context, c ctrlclient.Client, obj objectLabels) (allowed bool, drillID, denyReason string, err error) {
	// Step 1: namespace drill-active? Cheap to check first because the
	// vast majority of admission calls outside drill windows skip the
	// gate entirely. Reuses the existing drillActive() helper from
	// sibling.go (same annotation contract; one source of truth).
	active, id, err := drillActive(ctx, c, obj.GetNamespace())
	if err != nil {
		return false, "", "", fmt.Errorf("drill-active lookup: %w", err)
	}
	if !active {
		return true, "", "", nil
	}

	// Step 2: scope-labeled? The gate only protects scope-labeled CRs.
	// A drill-active namespace may still host unrelated Stream / KV
	// CRs whose chart-driven updates must keep flowing. Checking the
	// label is a pure in-memory test on the incoming object — no API
	// roundtrip.
	if !isScopeLabeled(obj.GetLabels()) {
		return true, id, "", nil
	}

	// Step 3: requester == operator SA? When the admission context is
	// missing (unit tests not exercising the rejection path), we
	// optimistically allow. Production webhooks ALWAYS have an
	// admission.Request on context — controller-runtime's
	// admission.Webhook.Handle sets it before invoking validators.
	username := requesterUsername(ctx)
	if username == "" {
		// Unit-test path without explicit request setup. Allow so
		// existing tests that pre-date the gate keep passing.
		return true, id, "", nil
	}
	if username == DRPOperatorServiceAccount {
		return true, id, "", nil
	}

	return false, id, fmt.Sprintf("user=%q drill-active=%q", username, id), nil
}

// isScopeLabeled reports whether `labels` carries
// `drp.cicada.io/nats-failover-scope` with a non-empty value. We don't
// require the value to be literal "true" — any non-empty value is treated
// as in-scope, matching the conservative read the operator uses on its
// side (the value is the audit/diagnostic trail; presence is the gate).
func isScopeLabeled(labels map[string]string) bool {
	if labels == nil {
		return false
	}
	v, ok := labels[ScopeLabel]
	return ok && v != ""
}

// requesterUsername returns the AdmissionReview requester username, or "" if
// the admission.Request is absent from ctx. The "" return is the unit-test
// signal — see drillActiveOperatorGate's step 3 for the rationale.
//
// Wrapped here (rather than inlined in the gate) so a future change to
// the user-resolution policy (e.g. honor `Groups` for system:masters
// bypass on a real human admin) localizes here without touching the gate
// decision logic.
func requesterUsername(ctx context.Context) string {
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return ""
	}
	return req.UserInfo.Username
}

// formatOperatorOnlyRejection builds the rejection error message for a
// gate-blocked admission. The kind / object name come from the calling
// validator so the user sees the specific CR that was rejected.
func formatOperatorOnlyRejection(kind, objName, denyReason string) error {
	return fmt.Errorf(
		"%s %q rejected by drill-active operator-only gate: %s. %s",
		kind, objName, denyReason, operatorOnlyRemediationHint,
	)
}

// _ keeps the authenticationv1 import live — UserInfo is the type powering
// admission.Request.UserInfo.Username read by requesterUsername. Without
// this anchor, future refactors that elide the gate's only direct
// authenticationv1 reference (the username constant comparison) would
// silently drop the import and any new field access (e.g. .Groups for the
// system:masters bypass discussed in requesterUsername) would surface as a
// compile error far from this file.
var _ = authenticationv1.UserInfo{}
