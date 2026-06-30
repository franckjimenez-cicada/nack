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
	"fmt"
	"strings"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// passiveRoleGate is the minimum surface shouldTranslatePassiveRole
// needs. Kept narrow so unit tests can stand up a fake without
// implementing the full JetStreamController interface — *jsController
// satisfies it transitively via its embedded client.Client + the new
// methods, and tests can pass a fake-client-backed stub.
type passiveRoleGate interface {
	client.Reader
	PassiveRoleTranslationEnabled() bool
	CrossRegionNATSDomain() string
	// ColdStartRoleDefaultsPassive reports whether an ABSENT local-role
	// annotation should be treated as passive (instead of the default
	// active). Set per-deployment on the SECONDARY region so that, before
	// the drp-operator has stamped a role on a fresh cluster, this region
	// fails CLOSED (mirror) rather than open (primary) — avoiding a
	// transient dual-primary window. The primary region leaves this off.
	ColdStartRoleDefaultsPassive() bool
}

// readLocalRole returns the value of `drp.cicada.io/local-role` on the
// supplied namespace, or "" when the annotation is absent / the
// namespace is missing. Decoupled from the feature flag — the role is
// load-bearing for safety guards (B1: refuse mirror→primary destructive
// recreate when ns is still passive but the feature flag was toggled
// off mid-life) even when translation itself is disabled.
//
// Errors other than NotFound bubble up so the caller can refuse to
// proceed; silent fallthrough on a transient API error could
// destructively recreate the very stream we were translating last
// reconcile.
func readLocalRole(ctx context.Context, g passiveRoleGate, namespace string) (string, error) {
	ns := &corev1.Namespace{}
	if err := g.Get(ctx, types.NamespacedName{Name: namespace}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("read namespace %q to evaluate local-role: %w", namespace, err)
	}
	return ns.Annotations[localRoleAnnotation], nil
}

// shouldTranslatePassiveRole reports whether the reconciler should rewrite
// the supplied CR's spec to mirror form before applying to NATS, plus
// the current ns local-role value.
//
// Translation fires only when ALL of the following hold:
//   - The controller has --enable-passive-role-translation set (feature gate).
//   - --cross-region-nats-domain is non-empty (we need it to build the
//     externalApiPrefix). Without it, translation would synthesize an
//     invalid mirror config — better to skip and leave the CR as authored.
//   - The CR's namespace carries `drp.cicada.io/local-role=passive`.
//
// The returned localRole is the raw annotation value REGARDLESS of the
// feature gate, so callers can detect the "ns is passive but flag is
// off" misconfig and refuse destructive operations. On namespace read
// error, returns (false, "", err) so the caller can fail the reconcile
// rather than silently treat as active.
func shouldTranslatePassiveRole(ctx context.Context, g passiveRoleGate, namespace string) (translate bool, localRole string, err error) {
	role, err := readLocalRole(ctx, g, namespace)
	if err != nil {
		return false, "", err
	}
	if !g.PassiveRoleTranslationEnabled() {
		return false, role, nil
	}
	if g.CrossRegionNATSDomain() == "" {
		return false, role, nil
	}
	// Cold-start hardening: when the annotation is ABSENT (the drp-operator
	// has not stamped a role yet on a fresh cluster) and this deployment is
	// configured to default unset→passive (the secondary region), treat it
	// as passive so we fail CLOSED to mirror rather than open to primary.
	// The RAW role ("") is still returned so the destructive-recreate guard
	// sees the true annotation state, not the synthesized default.
	effective := role
	if effective == "" && g.ColdStartRoleDefaultsPassive() {
		effective = localRolePassive
	}
	return effective == localRolePassive, role, nil
}

// scopeLabel is the label key that marks a Stream / KeyValue CR as
// participating in DRP cross-region failover. Mirrors
// webhook.ScopeLabel (kept as a separate const here to avoid the
// controller package importing the webhook package). ACTIVE-role
// translation (the inverse of passive translation) only ever fires on
// CRs carrying this label with a non-empty value, so steady-state
// primaries in non-DRP namespaces are never touched.
const scopeLabel = "drp.cicada.io/nats-failover-scope"

// isScopeLabeled reports whether the CR carries a non-empty
// drp.cicada.io/nats-failover-scope label. ACTIVE-role translation is
// strictly gated on this so it only operates on the DRP-managed scope
// set (the same set the operator demote/promote enumerates), never an
// arbitrary primary.
func isScopeLabeled(labels map[string]string) bool {
	return strings.TrimSpace(labels[scopeLabel]) != ""
}

// shouldConvertActiveRole reports whether the reconciler should perform
// an ACTIVE-role-translation IN-PLACE promote of this CR: convert a
// server-side MIRROR back to a PRIMARY (drop Mirror, set authored
// subjects) WITHOUT deleting the stream, preserving all replicated
// messages.
//
// This is the INVERSE of passive-role translation (fork PR #8 converts a
// primary-form CR → server mirror when local-role=passive). PR #8 had no
// inverse: when local-role flips back to active, nothing converted an
// existing server mirror back to a primary, so the drp-operator promote
// (which under passive-translation finds the CRs already primary-form and
// mutates nothing) established NO primary — both regions stayed server-
// side mirrors. This predicate closes that gap.
//
// Fires only when ALL hold:
//   - The CR is scope-labeled (drp.cicada.io/nats-failover-scope set).
//   - The namespace local-role is NOT passive (active, or absent =
//     active default — a cluster with no role declared serves its
//     authored primaries).
//   - The authored (effective) spec is PRIMARY form (Mirror == nil):
//     active-translation never creates a mirror, it only un-does one.
//   - The SERVER stream is currently a MIRROR (serverIsMirror): there is
//     a mirror to convert. A server already-primary is the steady state
//     and is left untouched (the normal update path converges it).
//
// effectiveSpecHasMirror is the post-passive-translation spec's Mirror
// presence; when local-role=passive the passive path already handles the
// CR and this predicate must not also fire (it won't — localRole==passive
// fails the role gate).
func shouldConvertActiveRole(scopeLabeled bool, localRole string, effectiveSpecHasMirror, serverIsMirror bool) bool {
	if !scopeLabeled {
		return false
	}
	if localRole == localRolePassive {
		return false
	}
	if effectiveSpecHasMirror {
		return false
	}
	return serverIsMirror
}

// passiveRoleGuardMsg builds the operator-facing message attached to
// Ready=Errored when the safety guard fires. Centralized so the
// proactive + reactive sites in both controllers stay symmetric.
func passiveRoleGuardMsg(namespace string, translationEnabled bool, domain string) string {
	return fmt.Sprintf(
		"refusing mirror→primary destructive recreate: namespace %q has %s=%s but the controller is configured to apply primary form (translation enabled=%t, domain=%q). Set --enable-passive-role-translation + --cross-region-nats-domain, or clear the namespace annotation before continuing.",
		namespace, localRoleAnnotation, localRolePassive,
		translationEnabled, domain,
	)
}

// passiveRoleWouldDemote reports whether the current reconcile state
// would, if it proceeded, destructively recreate a NATS-server mirror
// stream back into primary form while the namespace is still annotated
// `local-role=passive`. This is the B1 hazard: an operator who toggles
// --enable-passive-role-translation off without first clearing the
// annotation would otherwise see the controller demote every translated
// mirror, losing in-flight replicated state and seeding split-brain.
//
// Both destructive-recreate sites (proactive flip detection AND reactive
// fallback from a mirror-incompatible UpdateConfiguration error) must
// gate on this predicate.
//
// The signature takes bools (rather than pointers to the Mirror fields)
// because the two controllers carry distinct server-side Mirror types —
// Stream uses *jsmapi.StreamSource (jsm.go) while KeyValue uses
// *jetstream.StreamSource (nats.go/jetstream). Booleans unify the
// presence test across both packages without dragging an interface
// through the call signature.
func passiveRoleWouldDemote(serverHasMirror, effectiveSpecHasMirror bool, localRole string) bool {
	return serverHasMirror && !effectiveSpecHasMirror && localRole == localRolePassive
}

// translateStreamSpecToMirror returns a deep-copied Stream spec with
// Subjects + Sources cleared and Mirror set to a config that pulls from
// the peer region's JetStream domain. The original spec (and therefore
// the in-cluster CR) is left untouched.
//
// Uses the generated DeepCopy so pointer fields (Placement,
// SubjectTransform, RePublish, ConsumerLimits, Metadata map) are
// genuinely independent — a shallow `*orig` copy would alias those into
// the returned value, and a well-intentioned downstream mutation could
// corrupt the live in-memory CR object. The translation contract
// ("K8s CR is untouched, server-side only") MUST hold against such
// future edits, not just today's careful caller.
//
// Caller is responsible for already having decided translation should
// fire (see shouldTranslatePassiveRole) — this function performs the
// transformation unconditionally on its inputs.
func translateStreamSpecToMirror(orig *api.StreamSpec, remoteDomain string) api.StreamSpec {
	translated := *orig.DeepCopy()
	translated.Subjects = nil
	translated.Sources = nil
	streamName := orig.Name
	translated.Mirror = &api.StreamSource{
		Name:                  streamName,
		ExternalAPIPrefix:     fmt.Sprintf("$JS.%s.API", remoteDomain),
		ExternalDeliverPrefix: fmt.Sprintf("deliver.%s.dr", streamName),
	}
	return translated
}

// translateStreamSpecToPrimary is the INVERSE of translateStreamSpecToMirror:
// it returns a deep-copied spec with Mirror + Sources cleared, leaving the
// authored Subjects intact, so the effective spec is PRIMARY form.
//
// Why this is needed: the DRP gitops authors the secondary region's scope
// streams in "mirror baseline" form — the in-cluster CR carries BOTH a Mirror
// (used while the region is passive) AND the authored Subjects (used once it is
// promoted). passive-role translation handles the passive direction (drop
// Subjects, set Mirror). Its inverse was missing: when the region flips ACTIVE,
// the CR's effective spec STILL had the authored Mirror, so shouldConvertActiveRole
// (which requires a primary-form effective spec) never fired and nack applied a
// Mirror+Subjects config the server rejects with 10034. Stripping the Mirror here
// makes the effective spec primary-form, so the in-place two-phase promote
// converges the server mirror to a primary WITHOUT deleting (messages preserved).
// The in-cluster CR is left untouched (server-side translation only).
func translateStreamSpecToPrimary(orig *api.StreamSpec) api.StreamSpec {
	translated := *orig.DeepCopy()
	translated.Mirror = nil
	translated.Sources = nil
	return translated
}

// shouldTranslateActiveRole reports whether the reconciler should strip a
// CR-authored Mirror so the effective spec is primary form — the ACTIVE-region
// inverse of shouldTranslatePassiveRole. Fires only when ALL hold:
//   - passive-role translation is feature-enabled (the same DRP gate; outside
//     DRP mode the CR is applied exactly as authored).
//   - the CR is scope-labeled (drp.cicada.io/nats-failover-scope set).
//   - the namespace is NOT effectively passive — i.e. local-role=active, or
//     absent and this deployment does NOT cold-start-default to passive (same
//     semantics shouldTranslatePassiveRole uses for the passive direction).
//   - the authored spec actually carries a Mirror to strip (a CR already in
//     primary form needs no translation; the normal path converges it).
func shouldTranslateActiveRole(translationEnabled, scopeLabeled bool, localRole string, coldStartDefaultsPassive, specHasMirror bool) bool {
	if !translationEnabled || !scopeLabeled || !specHasMirror {
		return false
	}
	effective := localRole
	if effective == "" && coldStartDefaultsPassive {
		effective = localRolePassive
	}
	return effective != localRolePassive
}

// translateKeyValueSpecToMirror is the KeyValue analog. The underlying
// JetStream stream that backs a KV bucket is named "KV_<bucket>", so the
// mirror's Name field uses that convention. The deliver prefix follows
// the chart's "deliver.kv.<bucket>.dr" pattern documented in
// gitops-platform-dev-stg/children/nacks-streams-sync values.
//
// Uses DeepCopy for the same reason as translateStreamSpecToMirror.
func translateKeyValueSpecToMirror(orig *api.KeyValueSpec, remoteDomain string) api.KeyValueSpec {
	translated := *orig.DeepCopy()
	translated.Sources = nil
	bucket := orig.Bucket
	translated.Mirror = &api.StreamSource{
		Name:                  kvStreamPrefix + bucket,
		ExternalAPIPrefix:     fmt.Sprintf("$JS.%s.API", remoteDomain),
		ExternalDeliverPrefix: fmt.Sprintf("deliver.kv.%s.dr", bucket),
	}
	return translated
}

// translateKeyValueSpecToPrimary is the KeyValue analog of
// translateStreamSpecToPrimary and the INVERSE of translateKeyValueSpecToMirror:
// it returns a deep-copied KeyValue spec with Mirror + Sources cleared, leaving
// the authored bucket config (Bucket, History, TTL, MaxBytes, Replicas,
// Compression, RePublish, …) intact, so the effective spec is PRIMARY form.
//
// Why this is needed (the KeyValue companion to the stream active-role fix, PR
// #17): the DRP gitops authors the secondary region's scope KV buckets in
// "mirror baseline" form — the in-cluster CR carries BOTH a Mirror (used while
// the region is passive) AND the authored bucket config (used once it is
// promoted). passive-role translation handles the passive direction (clear
// Sources, set Mirror). Its inverse was missing for KeyValue: when the region
// flips ACTIVE, the CR's effective spec STILL carried the authored Mirror, so
// shouldConvertActiveRole (which requires a primary-form effective spec) never
// fired and the scope KV's backing stream stayed a server-side mirror with no
// primary — exactly the prod failover gap on pre-approval-risk-bucket /
// quotefeed-bucket. Stripping the Mirror here makes the effective spec
// primary-form, so the in-place UpdateKeyValue promote converges the server
// mirror to a primary WITHOUT deleting the KV_<bucket> backing stream (keys
// preserved). The in-cluster CR is left untouched (server-side translation
// only).
//
// translateKeyValueSpecToMirror SETS Mirror and clears Sources; this inverse
// clears both Mirror and Sources, mirroring the Stream version's contract
// exactly. Uses the generated DeepCopy so pointer fields stay independent and
// the original spec / in-cluster CR is never mutated.
func translateKeyValueSpecToPrimary(orig *api.KeyValueSpec) api.KeyValueSpec {
	translated := *orig.DeepCopy()
	translated.Mirror = nil
	translated.Sources = nil
	return translated
}
