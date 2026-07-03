/*
Copyright 2025.

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

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/jsm.go"
	jsmapi "github.com/nats-io/jsm.go/api"
	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	"github.com/nats-io/nats.go/jetstream"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// StreamReconciler reconciles a Stream object
type StreamReconciler struct {
	Scheme *runtime.Scheme

	JetStreamController
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// It performs three main operations:
// - Initialize finalizer and ready condition if not present
// - Delete stream if it is marked for deletion.
// - Create or Update the stream
//
// A call to reconcile may perform only one action, expecting the reconciliation to be triggered again by an update.
// For example: Setting the finalizer triggers a second reconciliation. Reconcile returns after setting the finalizer,
// to prevent parallel reconciliations performing the same steps.
func (r *StreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := klog.FromContext(ctx)

	if ok := r.ValidNamespace(req.Namespace); !ok {
		log.Info("Controller restricted to namespace, skipping reconciliation.")
		return ctrl.Result{}, nil
	}

	// Fetch stream resource
	stream := &api.Stream{}
	if err := r.Get(ctx, req.NamespacedName, stream); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Stream resource deleted.", "streamName", req.NamespacedName.String())
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get stream resource '%s': %w", req.NamespacedName.String(), err)
	}

	log = log.WithValues("streamName", stream.Spec.Name)

	// Update ready status to unknown when no status is set
	if len(stream.Status.Conditions) == 0 {
		log.Info("Setting initial ready condition to unknown.")
		stream.Status.Conditions = updateReadyCondition(stream.Status.Conditions, v1.ConditionUnknown, stateReconciling, "Starting reconciliation")
		err := r.Status().Update(ctx, stream)
		if err != nil {
			// If we get a conflict error, another reconciliation has already updated the status.
			// Just requeue and let the next reconciliation handle it.
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("set condition unknown: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Check Deletion
	markedForDeletion := stream.GetDeletionTimestamp() != nil
	if markedForDeletion {
		if controllerutil.ContainsFinalizer(stream, streamFinalizer) {
			err := r.deleteStream(ctx, log, stream)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete stream: %w", err)
			}
		} else {
			log.Info("Stream marked for deletion and already finalized. Ignoring.")
		}

		return ctrl.Result{}, nil
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(stream, streamFinalizer) {
		log.Info("Adding stream finalizer.")
		if ok := controllerutil.AddFinalizer(stream, streamFinalizer); !ok {
			return ctrl.Result{}, errors.New("failed to add finalizer to stream resource")
		}

		if err := r.Update(ctx, stream); err != nil {
			return ctrl.Result{}, fmt.Errorf("update stream resource to add finalizer: %w", err)
		}
		// After we have added the finalizer, we need to requeue to make sure we reconcile the
		// rest of the object. Just updating metadata won't make the API server update generation
		// so the update above shouldn't trigger a new reconciliation.
		return ctrl.Result{Requeue: true}, nil
	}

	// Create or update stream
	if err := r.createOrUpdate(ctx, log, stream); err != nil {
		return ctrl.Result{}, fmt.Errorf("create or update: %s", err)
	}

	return ctrl.Result{RequeueAfter: r.RequeueInterval()}, nil
}

func (r *StreamReconciler) deleteStream(ctx context.Context, log logr.Logger, stream *api.Stream) error {
	// Set status to false
	stream.Status.Conditions = updateReadyCondition(stream.Status.Conditions, v1.ConditionFalse, stateFinalizing, "Performing finalizer operations.")
	if err := r.Status().Update(ctx, stream); err != nil {
		return fmt.Errorf("update ready condition: %w", err)
	}

	storedState, err := getStoredStreamState(stream)
	if err != nil {
		log.Error(err, "Failed to fetch stored state.")
	}

	if !stream.Spec.PreventDelete && !r.ReadOnly() {
		log.Info("Deleting stream.")
		err := r.WithJSMClient(stream.Spec.ConnectionOpts, stream.Namespace, func(js *jsm.Manager) error {
			_, err := getServerStreamState(js, stream)
			// If we have no known state for this stream it has never been reconciled.
			// If we are also receiving an error fetching state, either the stream does not exist
			// or this resource config is invalid.
			if err != nil && storedState == nil {
				return nil
			}

			return js.DeleteStream(stream.Spec.Name)
		})
		if jsmapi.IsNatsErr(err, JSStreamNotFoundErr) {
			log.Info("Stream does not exist, unable to delete.", "streamName", stream.Spec.Name)
		} else if err != nil && storedState == nil {
			log.Info("Stream not reconciled and no state received from server. Removing finalizer.")
		} else if err != nil {
			return fmt.Errorf("delete stream during finalization: %w", err)
		}
	} else {
		log.Info("Skipping stream deletion.",
			"preventDelete", stream.Spec.PreventDelete,
			"read-only", r.ReadOnly(),
		)
	}

	log.Info("Removing stream finalizer.")
	if ok := controllerutil.RemoveFinalizer(stream, streamFinalizer); !ok {
		return errors.New("failed to remove stream finalizer")
	}
	if err := r.Update(ctx, stream); err != nil {
		return fmt.Errorf("remove finalizer: %w", err)
	}

	return nil
}

// CreateOrUpdate is a thin exported delegation to createOrUpdate — the
// reconcile-core step that evaluates passive/active local-role translation
// (drp.cicada.io/local-role) and applies the resulting (possibly translated)
// Stream spec to the NATS server. It exists SOLELY so the non-internal
// pkg/roletranslate test-support package can drive this EXACT production
// code path from outside the module (Go's internal/ import restriction
// otherwise blocks external harnesses, e.g. drp-operator's integration
// tests, from reaching it) without duplicating the translation logic.
// Reconcile (the real controller-runtime entry point) calls createOrUpdate
// directly and never goes through this method.
func (r *StreamReconciler) CreateOrUpdate(ctx context.Context, log logr.Logger, stream *api.Stream) error {
	return r.createOrUpdate(ctx, log, stream)
}

func (r *StreamReconciler) createOrUpdate(ctx context.Context, log logr.Logger, stream *api.Stream) error {
	// gated tracks whether the BackupRequired condition was raised
	// during this reconcile pass. The outer block uses it to decide
	// whether to short-circuit the success-path Ready=True update.
	var gated bool
	var gatedReason, gatedMessage string
	// passiveRoleGuardBlocked is set when the B1 safety guard fires:
	// namespace says passive but the controller would otherwise demote a
	// translated mirror back to primary (feature flag toggled off
	// mid-life). Treated the same as `gated` — status surfaces Ready=False
	// with a clear reason and the success path is skipped.
	var passiveRoleGuardBlocked bool
	var passiveRoleGuardMessage string

	// Evaluate passive-role translation BEFORE touching NATS so the
	// rest of the reconcile (server state probe, mirror-flip detection,
	// backup gate, UpdateConfiguration) all see the same "effective"
	// spec. The original stream object is left untouched — translation
	// is server-side only, so ArgoCD continues to see the chart's
	// primary form on the CR and stays Synced/Healthy.
	//
	// localRole is returned regardless of the feature gate so the
	// downstream destructive-recreate guard can refuse to demote a
	// translated mirror back to primary when the ns is still passive
	// (B1: protects against operator toggling the feature flag off mid-life).
	translatePassive, localRole, translateErr := shouldTranslatePassiveRole(ctx, r.JetStreamController, stream.Namespace)
	if translateErr != nil {
		// Reading the namespace failed (network blip / RBAC gap). Surface
		// as a reconcile error rather than silently treating as active —
		// silent fallthrough could destructively recreate a stream we
		// were meant to keep mirroring.
		stream.Status.Conditions = updateReadyCondition(stream.Status.Conditions, v1.ConditionFalse, stateErrored, translateErr.Error())
		if err := r.Status().Update(ctx, stream); err != nil {
			log.Error(err, "Failed to update ready condition to Errored.")
		}
		return translateErr
	}
	effectiveSpec := &stream.Spec
	if translatePassive {
		translated := translateStreamSpecToMirror(&stream.Spec, r.CrossRegionNATSDomain())
		effectiveSpec = &translated
		log.Info("Passive-role translation active: applying mirror config to NATS server (K8s CR untouched).",
			"localRole", localRolePassive,
			"remoteDomain", r.CrossRegionNATSDomain(),
			"namespace", stream.Namespace,
			"streamName", stream.Spec.Name,
		)
	} else if shouldTranslateActiveRole(
		r.JetStreamController.PassiveRoleTranslationEnabled(),
		isScopeLabeled(stream.Labels),
		localRole,
		r.JetStreamController.ColdStartRoleDefaultsPassive(),
		stream.Spec.Mirror != nil,
	) {
		// ACTIVE-role translation: the gitops "mirror baseline" CR carries both a
		// Mirror and the authored Subjects. Strip the Mirror so the effective spec
		// is primary form; shouldConvertActiveRole then fires and the two-phase
		// in-place promote converges the server mirror to a primary (messages
		// preserved, never deleted). The in-cluster CR is left untouched.
		translated := translateStreamSpecToPrimary(&stream.Spec)
		effectiveSpec = &translated
		log.Info("Active-role translation: stripping CR-authored mirror so the effective spec is primary form (keeping authored subjects).",
			"localRole", localRole,
			"namespace", stream.Namespace,
			"streamName", stream.Spec.Name,
		)
	}

	// CreateOrUpdateStream is called on every reconciliation when the stream is not to be deleted.
	err := r.WithJSMClient(stream.Spec.ConnectionOpts, stream.Namespace, func(js *jsm.Manager) error {
		storedState, err := getStoredStreamState(stream)
		if err != nil {
			log.Error(err, "Failed to fetch stored stream state")
		}

		serverState, err := getServerStreamState(js, stream)
		if err != nil {
			return err
		}

		// mustPromoteInPlace is set when the proactive flip block detects a
		// mirror→primary promote. It (a) bypasses the converged-skip below so
		// the in-place UpdateConfiguration actually runs even when stored==server
		// (both mirror) and ObservedGeneration==Generation, and (b) triggers the
		// post-update server-side re-read that asserts the mirror was genuinely
		// dropped (never trust UpdateConfiguration's nil return for a promote).
		var mustPromoteInPlace bool

		// Map effective spec (possibly translated) to stream targetConfig,
		// passing current server state for context.
		targetConfig, err := streamSpecToConfig(effectiveSpec, serverState)
		if err != nil {
			return fmt.Errorf("map spec to stream targetConfig: %w", err)
		}

		// Proactive source<->mirror flip handling.
		//
		// NATS forbids switching an existing stream between source-mode (no
		// spec.mirror, possibly with subjects) and mirror-mode (spec.mirror
		// set) via UpdateConfiguration. If we see the CR has flipped relative
		// to the server, the only correct path is to delete the server stream
		// and re-create from the CR. This is opt-in to preserve upstream
		// semantics.
		//
		// When --require-backup-confirmation is set we additionally gate
		// the destructive delete on either (a) the cross-region peer
		// already holding this stream's data, or (b) an external operator
		// confirming a backup via the configured annotation. See
		// streamBackupGate below for the full predicate.
		// Pass the EFFECTIVE spec (possibly translated to mirror form) into
		// the flip detector so that, under passive-role translation, the
		// "old server is primary, new spec is mirror" path is recognized
		// and the destructive recreate runs against the synthesized mirror
		// — not the chart's primary form.
		// ACTIVE-role translation (the inverse of passive-role translation,
		// fork PR #8): when this cluster's nats namespace is ACTIVE (not
		// passive) and a scope-labeled, primary-form CR's SERVER stream is
		// still a MIRROR, convert it back to a PRIMARY IN PLACE (drop the
		// server-side Mirror + set authored subjects via the same
		// UpdateConfiguration mechanism PR #11 uses for the mirror→primary
		// promote — it retains all replicated messages). This is the missing
		// inverse that left the drp-operator promote unable to establish a
		// primary under passive-translation (the live E→W failback bug). It
		// fires INDEPENDENTLY of --mirror-recreate-on-conflict: that flag
		// gates the DESTRUCTIVE primary→mirror recreate, but active-promote is
		// non-destructive (in place), so it must not require the destructive
		// flag to be enabled. Strictly gated (scope-labeled + active +
		// server-currently-mirror + spec-primary) so steady-state primaries
		// are never touched.
		activePromote := serverState != nil && !stream.Spec.PreventUpdate && !r.ReadOnly() &&
			shouldConvertActiveRole(isScopeLabeled(stream.Labels), localRole, effectiveSpec.Mirror != nil, serverState.Mirror != nil)
		if activePromote {
			log.Info("Active-role translation: scope stream's server side is a mirror but local-role is active and the CR is primary-form; converting mirror→primary IN PLACE (preserving messages).",
				"streamName", stream.Spec.Name,
				"namespace", stream.Namespace,
				"localRole", localRole,
			)
		}
		if serverState != nil && !stream.Spec.PreventUpdate && !r.ReadOnly() && (activePromote || (r.MirrorRecreateOnConflict() && streamMirrorFlipped(serverState, effectiveSpec))) {
			// B1 safety guard: refuse the destructive recreate when the
			// flip direction is mirror → primary AND the namespace is
			// still annotated `local-role=passive`. The only realistic
			// way to reach this branch is operator misconfig (feature
			// flag toggled off while the ns annotation still says
			// passive) — destroying the mirror would lose the in-flight
			// replicated state and seed split-brain. Surface as
			// non-Ready + a clear status reason and bail.
			if passiveRoleWouldDemote(serverState.Mirror != nil, effectiveSpec.Mirror != nil, localRole) {
				passiveRoleGuardBlocked = true
				passiveRoleGuardMessage = passiveRoleGuardMsg(stream.Namespace, r.PassiveRoleTranslationEnabled(), r.CrossRegionNATSDomain())
				log.Error(nil, "Passive-role safety guard fired; skipping proactive destructive recreate.",
					"namespace", stream.Namespace, "streamName", stream.Spec.Name,
				)
				return nil
			}
			// DATA-PRESERVING mirror→primary promote (the data-loss fix).
			// When the server stream is a mirror and the effective spec drops
			// the mirror to become a primary, this is the DRP promote. It is
			// achievable IN PLACE (drop Mirror + set Subjects via
			// UpdateConfiguration) on nats-server WITHOUT deleting the server
			// stream, so the replicated messages survive. Do NOT take the
			// destructive delete branch here: fall through to the normal
			// update path below, which now emits an explicit Mirror=nil opt
			// (see streamSpecToConfig) so the in-place update converges instead
			// of being rejected as a mirror flip. The streamBackupGate /
			// delete are reserved for the genuinely-destructive primary→mirror
			// direction. (The B1 passiveRoleWouldDemote guard above has already
			// run, so a flag-toggled-off-while-ns-passive misconfig is still
			// refused before we get here.)
			if streamMirrorToPrimaryFlip(serverState, effectiveSpec) {
				log.Info("Mirror→primary promote detected; converting IN PLACE (preserving messages) instead of delete+recreate.",
					"streamName", stream.Spec.Name,
					"serverMsgsPreserved", true,
					"translated", translatePassive,
				)
				// Leave serverState set so the update path runs UpdateConfiguration
				// (in place) rather than NewStream. streamSpecToConfig clears the
				// server-side Mirror so the update drops it.
				//
				// SILENT-NO-OP FIX: force the update to actually run. The
				// converged-skip check below (storedState==serverState &&
				// ObservedGeneration==Generation → return nil) would otherwise
				// short-circuit this promote entirely: under passive-role
				// translation the stored-state annotation holds the MIRROR config
				// (persisted by a prior passive-era reconcile) and the server is
				// still that same mirror, so the diff is empty and the reconcile
				// returns nil WITHOUT ever calling UpdateConfiguration. That is the
				// live "logs every 60s, no error, is_mirror stays true" loop. The
				// CR generation does NOT bump on an ns local-role flip, so
				// ObservedGeneration==Generation holds. Set mustPromoteInPlace so
				// the converged-skip is bypassed and UpdateConfiguration runs.
				mustPromoteInPlace = true
			} else {
				gateFires, reason, msg, gateErr := r.streamBackupGate(js, stream, serverState)
				if gateErr != nil {
					return fmt.Errorf("evaluate backup gate: %w", gateErr)
				}
				if gateFires {
					log.Info("Destructive recreate gated by --require-backup-confirmation; awaiting external backup.",
						"reason", reason, "message", msg,
					)
					gated = true
					gatedReason = reason
					gatedMessage = msg
					return nil
				}
				log.Info("Source<->mirror flip detected (primary→mirror); force-recreating Stream.",
					"specHasMirror", effectiveSpec.Mirror != nil,
					"serverHasMirror", serverState.Mirror != nil,
					"translated", translatePassive,
				)
				if delErr := js.DeleteStream(stream.Spec.Name); delErr != nil && !jsmapi.IsNatsErr(delErr, JSStreamNotFoundErr) {
					return fmt.Errorf("force-delete on mirror flip: %w", delErr)
				}
				serverState = nil
			}
		}

		// Check against known state. Skip Update if converged.
		// Storing returned state from the server avoids have to
		// check default values or call Update on already converged resources
		//
		// NEVER skip when a mirror→primary promote is pending: the stored state
		// and the live server are BOTH the mirror config (diff == ""), so this
		// short-circuit would silently swallow the promote — the production
		// silent-no-op bug. mustPromoteInPlace forces the update through.
		if !mustPromoteInPlace && storedState != nil && serverState != nil && stream.Status.ObservedGeneration == stream.Generation {
			diff := compareConfigState(storedState, serverState)

			if diff == "" {
				return nil
			}

			log.Info("Stream config drifted from desired state.", "diff", diff)
		}

		if r.ReadOnly() {
			log.Info("Skipping stream creation or update.",
				"read-only", r.ReadOnly(),
			)
			return nil
		}

		var updatedStream *jsm.Stream
		err = nil

		if serverState == nil {
			log.Info("Creating Stream.")
			updatedStream, err = js.NewStream(stream.Spec.Name, targetConfig...)
			if err != nil {
				return err
			}
		} else if !stream.Spec.PreventUpdate {
			log.Info("Updating Stream.")
			s, err := js.LoadStream(stream.Spec.Name)
			if err != nil {
				return err
			}

			// TWO-PHASE in-place mirror→primary promote (the 10034 fix).
			//
			// A single UpdateConfiguration that BOTH drops the mirror AND sets
			// Subjects on a stream the server STILL sees as a mirror is rejected
			// by some nats-server versions with 10034 "stream mirrors can not
			// contain subjects" — you cannot add subjects to a stream while it is
			// still a mirror in one step. (The embedded v2.14.0 used by the unit
			// tests happens to accept the one-shot update; the PROD server did
			// not, stranding the DRP flip at PromotingDestination with 32/37
			// streams looping 10034 forever.) Split the promote into two
			// sequential, DATA-SAFE updates — NEVER a delete, so all replicated
			// messages survive:
			//
			//   Phase 1 — un-mirror: UpdateConfiguration with Mirror=nil,
			//     Sources=nil and Subjects=nil. The server stream becomes a
			//     standalone (non-mirror) stream that RETAINS its messages and has
			//     no subjects yet — a valid config that does not trip 10034.
			//   Phase 2 — add subjects: re-LoadStream (fresh, now-standalone
			//     server state) and UpdateConfiguration with the authored target
			//     config. The stream is no longer a mirror, so 10034 cannot fire.
			//
			// Guarded so it fires ONLY for a genuine promote whose target has
			// subjects: mustPromoteInPlace (server mirror + spec primary, set by
			// the flip block above) AND the live server is still a mirror AND the
			// target config carries Subjects. A promote whose target legitimately
			// has no subjects already converges in a single update, so it keeps
			// the normal single-update path. The B1 passiveRoleWouldDemote guard
			// and the 10065 retryable handling are upstream/below and unchanged.
			//
			// Idempotency: reconciles are re-entrant. If only phase 1 lands (e.g.
			// the pass errors before phase 2, or phase 2 hits the transient 10065
			// overlap), the next reconcile sees a now-standalone stream — at which
			// point mustPromoteInPlace/serverState.Mirror is false, this block is
			// skipped, and the ordinary single update applies the subjects. So the
			// promote converges whether both phases run in one pass or across two.
			if mustPromoteInPlace && serverState.Mirror != nil && targetConfigHasSubjects(effectiveSpec) {
				log.Info("Two-phase in-place mirror→primary promote: phase 1 un-mirror (drop Mirror/Sources, no subjects yet) to avoid 10034 before adding subjects.",
					"streamName", stream.Spec.Name,
				)
				if p1err := s.UpdateConfiguration(*serverState, unmirrorStreamOpts()...); p1err != nil {
					// Phase 1 itself can hit the transient 10065 overlap if the
					// source has not released subjects yet; surface it as the same
					// retryable error the single-update path uses (NOT destructive).
					if isSubjectOverlapErr(p1err) {
						log.Info("Phase-1 un-mirror rejected by NATS as subject-overlap (10065); source has not released subjects yet. Retrying in place (NOT deleting — preserves data).",
							"streamName", stream.Spec.Name, "natsErr", p1err.Error(),
						)
						return fmt.Errorf("phase-1 un-mirror of stream %q blocked by subject overlap (10065): source still owns the subjects (demote not yet converged); will retry without destroying data: %w", stream.Spec.Name, p1err)
					}
					return fmt.Errorf("phase-1 un-mirror of stream %q: %w", stream.Spec.Name, p1err)
				}
				// Re-load fresh server state: phase 2 must operate on the
				// now-standalone (non-mirror) stream, not the stale mirror config.
				s, err = js.LoadStream(stream.Spec.Name)
				if err != nil {
					return fmt.Errorf("reload stream %q after phase-1 un-mirror: %w", stream.Spec.Name, err)
				}
				freshState := s.Configuration()
				serverState = &freshState
				log.Info("Two-phase in-place mirror→primary promote: phase 2 add authored subjects to the now-standalone stream.",
					"streamName", stream.Spec.Name,
				)
			}

			err = s.UpdateConfiguration(*serverState, targetConfig...)
			if err != nil {
				// DATA-LOSS GUARD (the promote fix): a subject-overlap rejection
				// (10065) during a mirror→primary promote is a TRANSIENT ORDERING
				// condition — the source still owns the subjects this destination
				// is claiming, because the source has not finished demoting to
				// mirror form yet. It is NOT a mirror-flip incompatibility. Force-
				// deleting + recreating here would destroy the very messages the
				// in-place promote preserves. Surface a retryable error so the
				// next reconcile re-attempts the in-place update once the source
				// has released the subjects (the DRP flip demotes the source
				// before promoting the destination, so the overlap self-clears).
				if isSubjectOverlapErr(err) {
					log.Info("In-place promote rejected by NATS as subject-overlap (10065); source has not released subjects yet. Retrying in place (NOT deleting — preserves data).",
						"streamName", stream.Spec.Name, "natsErr", err.Error(),
					)
					return fmt.Errorf("in-place mirror→primary promote of stream %q blocked by subject overlap (10065): source still owns the subjects (demote not yet converged); will retry without destroying data: %w", stream.Spec.Name, err)
				}
				// Reactive fallback: if NATS rejects because the requested
				// change requires source<->mirror flip, force-delete and
				// re-create. Bounded to a single retry.
				//
				// NEVER take this destructive branch for a mirror→primary
				// promote: that direction is achievable in place (drop Mirror)
				// and deleting would lose the replicated data. Only the
				// genuinely-impossible primary→mirror direction may recreate.
				if r.MirrorRecreateOnConflict() && isMirrorIncompatibleErr(err) && !streamMirrorToPrimaryFlip(serverState, effectiveSpec) {
					// B1 safety guard (reactive site): the proactive
					// branch's predicate did not fire (maybe serverState
					// wasn't yet mirror at the proactive check), but
					// NATS's mirror-incompatible error tells us the
					// effective-spec / server pair is mismatched. Refuse
					// the reactive delete on the same condition.
					//
					// serverState here is the read from before
					// UpdateConfiguration was attempted. A concurrent
					// external mutation between read and update could
					// stale this — but a misfire is recoverable: false
					// negative falls through to streamBackupGate (which
					// itself blocks destructive recreate); false positive
					// just costs one extra Ready=Errored cycle before
					// the next reconcile re-reads fresh state.
					if passiveRoleWouldDemote(serverState.Mirror != nil, effectiveSpec.Mirror != nil, localRole) {
						passiveRoleGuardBlocked = true
						passiveRoleGuardMessage = passiveRoleGuardMsg(stream.Namespace, r.PassiveRoleTranslationEnabled(), r.CrossRegionNATSDomain())
						log.Error(nil, "Passive-role safety guard fired; skipping reactive destructive recreate.",
							"namespace", stream.Namespace, "streamName", stream.Spec.Name, "natsErr", err.Error(),
						)
						return nil
					}
					gateFires, reason, msg, gateErr := r.streamBackupGate(js, stream, serverState)
					if gateErr != nil {
						return fmt.Errorf("evaluate backup gate: %w", gateErr)
					}
					if gateFires {
						log.Info("Reactive recreate gated by --require-backup-confirmation; awaiting external backup.",
							"reason", reason, "message", msg, "natsErr", err.Error(),
						)
						gated = true
						gatedReason = reason
						gatedMessage = msg
						return nil
					}
					log.Info("UpdateConfiguration rejected by NATS as mirror-incompatible; force-recreating Stream.",
						"err", err.Error(),
					)
					if delErr := js.DeleteStream(stream.Spec.Name); delErr != nil && !jsmapi.IsNatsErr(delErr, JSStreamNotFoundErr) {
						return fmt.Errorf("force-delete after mirror-incompatible update: %w", delErr)
					}
					updatedStream, err = js.NewStream(stream.Spec.Name, targetConfig...)
					if err != nil {
						return fmt.Errorf("re-create after mirror-incompatible update: %w", err)
					}
				} else {
					return err
				}
			} else {
				updatedStream, err = js.LoadStream(stream.Spec.Name)
				if err != nil {
					return err
				}

				// POST-UPDATE VERIFICATION (the silent-no-op closer): never
				// trust UpdateConfiguration's nil return for a mirror→primary
				// promote. nats-server (and some merge paths) can ACK an update
				// while leaving the server-side Mirror in place — the live bug
				// where the reconcile logged success every ~60s but is_mirror
				// stayed true. Re-read the freshly-loaded server config and, if
				// this was a promote, assert the mirror is genuinely gone. If the
				// mirror survives, return a RETRYABLE error so the next reconcile
				// re-attempts — and NEVER mark the CR Ready while the server still
				// shows a mirror.
				if mustPromoteInPlace && updatedStream.Configuration().Mirror != nil {
					log.Error(nil, "In-place mirror→primary promote did NOT drop the server-side mirror despite a nil UpdateConfiguration return; will retry (NOT marking Ready).",
						"streamName", stream.Spec.Name,
					)
					return fmt.Errorf("in-place mirror→primary promote of stream %q reported success but server still shows a mirror; retrying", stream.Spec.Name)
				}

				diff := compareConfigState(updatedStream.Configuration(), *serverState)
				log.Info("Updated Stream.", "diff", diff)
			}
		} else {
			log.Info("Skipping Stream update.",
				"preventUpdate", stream.Spec.PreventUpdate,
			)
		}

		if updatedStream != nil {
			// Store known state in annotation
			updatedState, err := json.Marshal(updatedStream.Configuration())
			if err != nil {
				return err
			}

			if stream.Annotations == nil {
				stream.Annotations = map[string]string{}
			}
			stream.Annotations[stateAnnotationStream] = string(updatedState)

			return r.Update(ctx, stream)
		}

		return nil
	})
	// Bake the PassiveRoleTranslated audit condition into the conditions
	// slice BEFORE any branch writes Status — so the closure-err path,
	// guard path, gate path, and success path all surface the same audit
	// signal. The condition value reflects whether translation was
	// ATTEMPTED this pass, not whether downstream NATS calls succeeded.
	if translatePassive {
		stream.Status.Conditions = updatePassiveRoleTranslatedCondition(
			stream.Status.Conditions, v1.ConditionTrue,
			"PassiveRole",
			fmt.Sprintf("Translated to mirror from $JS.%s.API", r.CrossRegionNATSDomain()),
		)
	} else {
		stream.Status.Conditions = removePassiveRoleTranslatedCondition(stream.Status.Conditions)
	}

	if err != nil {
		err = fmt.Errorf("create or update stream: %w", err)
		stream.Status.Conditions = updateReadyCondition(stream.Status.Conditions, v1.ConditionFalse, stateErrored, err.Error())
		if err := r.Status().Update(ctx, stream); err != nil {
			log.Error(err, "Failed to update ready condition to Errored.")
		}
		return err
	}

	if passiveRoleGuardBlocked {
		// Mirror→primary destructive recreate refused because the ns is
		// still passive. Do NOT bump observedGeneration — the next
		// reconcile should re-evaluate once the operator clears the
		// misconfig (flip the flag, or clear the annotation).
		stream.Status.Conditions = updateReadyCondition(
			stream.Status.Conditions, v1.ConditionFalse, stateErrored, passiveRoleGuardMessage,
		)
		if err := r.Status().Update(ctx, stream); err != nil {
			return fmt.Errorf("update Ready=Errored after passive-role guard: %w", err)
		}
		return nil
	}

	if gated {
		// The destructive recreate path was blocked by the backup gate.
		// Surface that via the BackupRequired condition + a non-Ready
		// status. Do NOT bump observedGeneration — the next reconcile
		// should re-evaluate against the latest spec/state.
		stream.Status.Conditions = updateBackupRequiredCondition(
			stream.Status.Conditions, v1.ConditionTrue, gatedReason, gatedMessage,
		)
		stream.Status.Conditions = updateReadyCondition(
			stream.Status.Conditions, v1.ConditionFalse, stateWaitingForBackup, gatedMessage,
		)
		if err := r.Status().Update(ctx, stream); err != nil {
			return fmt.Errorf("update BackupRequired condition: %w", err)
		}
		return nil
	}

	// Clear any stale BackupRequired condition that may have been
	// raised on a prior reconcile — the destructive recreate either
	// completed or was never needed this pass.
	stream.Status.Conditions = removeBackupRequiredCondition(stream.Status.Conditions)

	// update the observed generation and ready status
	stream.Status.ObservedGeneration = stream.Generation
	stream.Status.Conditions = updateReadyCondition(
		stream.Status.Conditions,
		v1.ConditionTrue,
		stateReady,
		"Stream successfully created or updated.",
	)
	err = r.Status().Update(ctx, stream)
	if err != nil {
		return fmt.Errorf("update ready condition: %w", err)
	}

	return nil
}

func getStoredStreamState(stream *api.Stream) (*jsmapi.StreamConfig, error) {
	var storedState *jsmapi.StreamConfig
	if state, ok := stream.Annotations[stateAnnotationStream]; ok {
		err := json.Unmarshal([]byte(state), &storedState)
		if err != nil {
			return nil, err
		}
	}

	return storedState, nil
}

// Fetch the current state of the stream from the server.
// JSStreamNotFoundErr is considered a valid response and does not return error
func getServerStreamState(jsm *jsm.Manager, stream *api.Stream) (*jsmapi.StreamConfig, error) {
	s, err := jsm.LoadStream(stream.Spec.Name)
	if jsmapi.IsNatsErr(err, JSStreamNotFoundErr) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	streamCfg := s.Configuration()
	return &streamCfg, nil
}

// targetConfigHasSubjects reports whether the (effective) primary spec the
// promote is converging toward carries any subjects. The two-phase split is
// only needed when subjects are being ADDED to a still-mirror stream — that is
// the exact shape nats-server rejects with 10034. A promote whose target has
// no subjects converges in a single update, so it must keep the normal path.
func targetConfigHasSubjects(spec *api.StreamSpec) bool {
	return spec != nil && len(spec.Subjects) > 0
}

// unmirrorStreamOpts builds the phase-1 ("un-mirror") UpdateConfiguration
// option set for the data-safe two-phase mirror→primary promote: drop the
// server-side Mirror and Sources and set NO subjects. Applied on top of the
// live (mirror-bearing) server config, it converts the stream to a standalone
// stream that RETAINS all its replicated messages and has no subjects yet — a
// valid config that does not trip 10034 "stream mirrors can not contain
// subjects". Phase 2 then adds the authored subjects to the now-standalone
// stream via the normal targetConfig. NEVER deletes — preserves data.
func unmirrorStreamOpts() []jsm.StreamOption {
	return []jsm.StreamOption{
		func(o *jsmapi.StreamConfig) error {
			o.Mirror = nil
			o.Sources = nil
			o.Subjects = nil
			return nil
		},
	}
}

func streamSpecToConfig(spec *api.StreamSpec, currentConfig *jsmapi.StreamConfig) ([]jsm.StreamOption, error) {
	opts := []jsm.StreamOption{
		jsm.StreamDescription(spec.Description),
		jsm.Subjects(spec.Subjects...),
		jsm.MaxConsumers(spec.MaxConsumers),
		jsm.MaxMessages(int64(spec.MaxMsgs)),
		jsm.MaxBytes(int64(spec.MaxBytes)),
		jsm.MaxMessageSize(int32(spec.MaxMsgSize)),
		jsm.Replicas(spec.Replicas),
		jsm.StreamMetadata(spec.Metadata),
	}

	// Set not directly mapped fields

	// retention
	switch spec.Retention {
	case "limits":
		opts = append(opts, jsm.LimitsRetention())
	case "interest":
		opts = append(opts, jsm.InterestRetention())
	case "workqueue":
		opts = append(opts, jsm.WorkQueueRetention())
	}

	// maxMsgsPerSubject
	if spec.MaxMsgsPerSubject > 0 {
		opts = append(opts, func(o *jsmapi.StreamConfig) error {
			o.MaxMsgsPer = int64(spec.MaxMsgsPerSubject)
			return nil
		})
	}

	// maxAge
	if spec.MaxAge != "" {
		d, err := time.ParseDuration(spec.MaxAge)
		if err != nil {
			return nil, fmt.Errorf("parse max age: %w", err)
		}
		opts = append(opts, jsm.MaxAge(d))
	}

	// storage
	switch spec.Storage {
	case "file":
		opts = append(opts, jsm.FileStorage())
	case "memory":
		opts = append(opts, jsm.MemoryStorage())
	}

	// discard
	switch spec.Discard {
	case "old":
		opts = append(opts, jsm.DiscardOld())
	case "new":
		opts = append(opts, jsm.DiscardNew())
	}

	// duplicateWindow
	if spec.DuplicateWindow != "" {
		d, err := time.ParseDuration(spec.DuplicateWindow)
		if err != nil {
			return nil, fmt.Errorf("parse duplicate window: %w", err)
		}
		opts = append(opts, jsm.DuplicateWindow(d))
	}

	// placement
	if spec.Placement != nil {
		if spec.Placement.Cluster != "" {
			opts = append(opts, jsm.PlacementCluster(spec.Placement.Cluster))
		}
		if spec.Placement.Tags != nil {
			opts = append(opts, jsm.PlacementTags(spec.Placement.Tags...))
		}
	} else if currentConfig != nil && currentConfig.Placement != nil {
		// Only clear placement if the current config has placement set.
		// This avoids triggering NATS error 10123: "can not move and scale a stream in a single update"
		// when we're only trying to change replicas.
		opts = append(opts, jsm.PlacementCluster(""))
	}
	// If spec.Placement is nil and currentConfig.Placement is also nil/empty,
	// we don't set any placement option, avoiding unnecessary placement changes.

	// mirror
	//
	// Always emit an explicit Mirror setter (NOT gated on spec.Mirror != nil).
	// UpdateConfiguration uses the live serverState as its base, so a setter
	// that is omitted leaves the server's previous value intact. For the
	// DATA-PRESERVING mirror→primary promote we MUST clear the server-side
	// Mirror in place — emitting `o.Mirror = nil` makes the in-place
	// UpdateConfiguration drop the mirror (verified to retain messages against
	// nats-server v2.14.0) instead of leaving the mirror set, which would make
	// the update a no-op flip and force the destructive delete+recreate path.
	if spec.Mirror != nil {
		ss, err := mapJSMStreamSource(spec.Mirror)
		if err != nil {
			return nil, fmt.Errorf("map mirror stream source: %w", err)
		}
		opts = append(opts, jsm.Mirror(ss))
	} else {
		opts = append(opts, func(o *jsmapi.StreamConfig) error {
			o.Mirror = nil
			return nil
		})
	}

	// sources
	//
	// Same always-set rationale as mirror: clear the server-side Sources when
	// the spec carries none, so an in-place update that drops sourcing
	// converges rather than retaining the server's previous sources.
	if spec.Sources != nil {
		streamSources := make([]*jsmapi.StreamSource, 0)
		for _, source := range spec.Sources {
			ss, err := mapJSMStreamSource(source)
			if err != nil {
				return nil, fmt.Errorf("map stream source: %w", err)
			}
			streamSources = append(streamSources, ss)
		}

		opts = append(opts, jsm.Sources(streamSources...))
	} else {
		opts = append(opts, func(o *jsmapi.StreamConfig) error {
			o.Sources = nil
			return nil
		})
	}

	// compression
	switch spec.Compression {
	case "s2":
		opts = append(opts, jsm.Compression(jsmapi.S2Compression))
	case "none":
		opts = append(opts, jsm.Compression(jsmapi.NoCompression))
	}

	// subjectTransform
	if spec.SubjectTransform != nil {
		st := &jsmapi.SubjectTransformConfig{
			Source:      spec.SubjectTransform.Source,
			Destination: spec.SubjectTransform.Dest,
		}

		opts = append(opts, jsm.SubjectTransform(st))
	}

	// rePublish
	if spec.RePublish != nil {
		r := &jsmapi.RePublish{
			Source:      spec.RePublish.Source,
			Destination: spec.RePublish.Destination,
			HeadersOnly: spec.RePublish.HeadersOnly,
		}

		opts = append(opts, jsm.Republish(r))
	}

	if spec.Sealed {
		opts = append(opts, func(o *jsmapi.StreamConfig) error {
			o.Sealed = spec.Sealed
			return nil
		})
	}

	// allowDirect, allowRollup handled by the bulk setter at the end of this function
	// so flipping any of them off propagates on update.

	// mirrorDirect — leave conditional: legacy controller doesn't toggle this on update either.
	if spec.MirrorDirect {
		opts = append(opts, jsm.MirrorDirect())
	}

	// discardPerSubject — keep helper for the side effect of forcing Discard=DiscardNew when enabled.
	if spec.DiscardPerSubject {
		opts = append(opts, jsm.DiscardNewPerSubject())
	}

	// firstSequence
	if spec.FirstSequence > 0 {
		opts = append(opts, jsm.FirstSequence(spec.FirstSequence))
	}

	// consumerLimits
	if spec.ConsumerLimits != nil {
		cl := jsmapi.StreamConsumerLimits{
			MaxAckPending: spec.ConsumerLimits.MaxAckPending,
		}
		if spec.ConsumerLimits.InactiveThreshold != "" {
			inactiveThreshold, err := time.ParseDuration(spec.ConsumerLimits.InactiveThreshold)
			if err != nil {
				return nil, fmt.Errorf("parse inactive threshold: %w", err)
			}
			cl.InactiveThreshold = inactiveThreshold
		}

		opts = append(opts, jsm.ConsumerLimits(cl))
	}

	// allowMsgTTL — server forbids disabling on update; emit only when enabling.
	if spec.AllowMsgTTL {
		opts = append(opts, jsm.AllowMsgTTL())
	}

	// subjectDeleteMarkerTtl
	if spec.SubjectDeleteMarkerTTL != "" {
		d, err := time.ParseDuration(spec.SubjectDeleteMarkerTTL)
		if err != nil {
			return nil, fmt.Errorf("parse subject delete marker TTL: %w", err)
		}
		opts = append(opts, jsm.SubjectDeleteMarkerTTL(d))
	}

	// Bulk setter for togglable bool fields. Always-set so flipping the CR back to false
	// propagates on update (UpdateConfiguration uses serverState as the base; opts that
	// don't touch a field leave its previous value intact).
	//
	// Only flags the NATS server permits toggling post-create are listed here.
	// Server-side one-way ("stream configuration update can not change ...",
	// "... can not be disabled", "... can not cancel ..."): NoAck, DenyDelete, DenyPurge,
	// Sealed, AllowMsgTTL, AllowMsgCounter, PersistMode — kept conditional below as
	// enable-only setters. AllowAtomicPublish and AllowMsgSchedules are also kept
	// conditional pending confirmation of server toggle-semantics. MirrorDirect mirrors
	// legacy behavior (not toggled on update).
	opts = append(opts, func(o *jsmapi.StreamConfig) error {
		o.AllowDirect = spec.AllowDirect
		o.RollupAllowed = spec.AllowRollup
		o.AllowBatchPublish = spec.AllowBatched
		return nil
	})

	// noAck — keep conditional (server-side toggle semantics not confirmed; matches
	// legacy enable-only behavior in the new controller).
	if spec.NoAck {
		opts = append(opts, jsm.NoAck())
	}

	// denyDelete — server forbids cancelling on update; emit only when enabling.
	if spec.DenyDelete {
		opts = append(opts, jsm.DenyDelete())
	}

	// denyPurge — server forbids cancelling on update; emit only when enabling.
	if spec.DenyPurge {
		opts = append(opts, jsm.DenyPurge())
	}

	// allowMsgCounter — server forbids changing post-create; emit only when enabling.
	if spec.AllowMsgCounter {
		opts = append(opts, func(o *jsmapi.StreamConfig) error {
			o.AllowMsgCounter = true
			return nil
		})
	}

	// allowAtomicPublish — emit only when enabling (toggle-off semantics not confirmed).
	if spec.AllowAtomicPublish {
		opts = append(opts, func(o *jsmapi.StreamConfig) error {
			o.AllowAtomicPublish = true
			return nil
		})
	}

	// allowMsgSchedules — emit only when enabling (server requires AllowRollup; toggle-off semantics not confirmed).
	if spec.AllowMsgSchedules {
		opts = append(opts, func(o *jsmapi.StreamConfig) error {
			o.AllowMsgSchedules = true
			return nil
		})
	}

	// persistMode — server forbids changing post-create; emit only the explicit values.
	switch spec.PersistMode {
	case "async":
		opts = append(opts, func(o *jsmapi.StreamConfig) error {
			o.PersistMode = jsmapi.AsyncPersistMode
			return nil
		})
	case "default":
		opts = append(opts, func(o *jsmapi.StreamConfig) error {
			o.PersistMode = jsmapi.DefaultPersistMode
			return nil
		})
	}

	return opts, nil
}

func mapStreamSource(ss *api.StreamSource) (*jetstream.StreamSource, error) {
	jss := &jetstream.StreamSource{
		Name:          ss.Name,
		FilterSubject: ss.FilterSubject,
	}

	if ss.OptStartSeq > 0 {
		jss.OptStartSeq = uint64(ss.OptStartSeq)
	}

	if ss.OptStartTime != "" {
		t, err := time.Parse(time.RFC3339, ss.OptStartTime)
		if err != nil {
			return nil, fmt.Errorf("parse opt start time: %w", err)
		}
		jss.OptStartTime = &t
	}

	if ss.ExternalAPIPrefix != "" || ss.ExternalDeliverPrefix != "" {
		jss.External = &jetstream.ExternalStream{
			APIPrefix:     ss.ExternalAPIPrefix,
			DeliverPrefix: ss.ExternalDeliverPrefix,
		}
	}

	for _, transform := range ss.SubjectTransforms {
		jss.SubjectTransforms = append(jss.SubjectTransforms, jetstream.SubjectTransformConfig{
			Source:      transform.Source,
			Destination: transform.Dest,
		})
	}

	if ss.Consumer != nil && ss.Consumer.Name != "" {
		jss.Consumer = &jetstream.StreamConsumerSource{
			Name:           ss.Consumer.Name,
			DeliverSubject: ss.Consumer.DeliverSubject,
		}
	}

	return jss, nil
}

func mapJSMStreamSource(ss *api.StreamSource) (*jsmapi.StreamSource, error) {
	jss := &jsmapi.StreamSource{
		Name:          ss.Name,
		FilterSubject: ss.FilterSubject,
	}

	if ss.OptStartSeq > 0 {
		jss.OptStartSeq = uint64(ss.OptStartSeq)
	}

	if ss.OptStartTime != "" {
		t, err := time.Parse(time.RFC3339, ss.OptStartTime)
		if err != nil {
			return nil, fmt.Errorf("parse opt start time: %w", err)
		}
		jss.OptStartTime = &t
	}

	if ss.ExternalAPIPrefix != "" || ss.ExternalDeliverPrefix != "" {
		jss.External = &jsmapi.ExternalStream{
			ApiPrefix:     ss.ExternalAPIPrefix,
			DeliverPrefix: ss.ExternalDeliverPrefix,
		}
	}

	for _, transform := range ss.SubjectTransforms {
		jss.SubjectTransforms = append(jss.SubjectTransforms, jsmapi.SubjectTransformConfig{
			Source:      transform.Source,
			Destination: transform.Dest,
		})
	}

	if ss.Consumer != nil && ss.Consumer.Name != "" {
		jss.Consumer = &jsmapi.StreamConsumerSource{
			Name:           ss.Consumer.Name,
			DeliverSubject: ss.Consumer.DeliverSubject,
		}
	}

	return jss, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// GenerationChangedPredicate is scoped to the Stream source ONLY (it was
	// previously a global WithEventFilter). The namespace watch needs its own
	// predicate because a local-role annotation change does NOT bump the
	// Namespace's generation, so a global GenerationChangedPredicate would
	// filter every namespace event out.
	c := mgr.GetClient()
	return ctrl.NewControllerManagedBy(mgr).
		For(&api.Stream{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&v1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, ns client.Object) []reconcile.Request {
				var list api.StreamList
				if err := c.List(ctx, &list, client.InNamespace(ns.GetName())); err != nil {
					return nil
				}
				reqs := make([]reconcile.Request, 0, len(list.Items))
				for i := range list.Items {
					reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
						Namespace: list.Items[i].Namespace, Name: list.Items[i].Name,
					}})
				}
				return reqs
			}),
			builder.WithPredicates(localRoleChangedPredicate()),
		).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
		}).
		Complete(r)
}

// streamMirrorFlipped reports whether the desired stream spec disagrees with
// the server-side stream config on whether it is a mirror. NATS forbids
// changing this on an existing stream — the only correct path is to delete
// and re-create.
func streamMirrorFlipped(serverState *jsmapi.StreamConfig, spec *api.StreamSpec) bool {
	if serverState == nil || spec == nil {
		return false
	}
	serverHasMirror := serverState.Mirror != nil
	specHasMirror := spec.Mirror != nil
	return serverHasMirror != specHasMirror
}

// streamMirrorToPrimaryFlip reports whether the desired transition is the
// DATA-PRESERVING mirror→primary direction: the server stream is currently a
// mirror and the (effective) spec drops the mirror to become an authored
// primary. This is the DRP "promote" — and on nats-server it is achievable
// IN PLACE via UpdateConfiguration (drop Mirror + set Subjects) WITHOUT
// deleting the server stream, so the messages already replicated into the
// mirror are RETAINED. Empirically verified against nats-server v2.14.0
// (TestStreamMirrorToPrimaryInPlacePreservesData).
//
// This is the inverse of the primary→mirror direction (server primary, spec
// mirror), which nats-server genuinely cannot satisfy in place — that one
// still requires the destructive delete + recreate.
//
// Why this distinction is load-bearing (the data-loss bug it fixes):
// the DRP flip's PromotingDestination converts a scope-labeled mirror CR to
// primary form. The prior code routed BOTH flip directions through
// js.DeleteStream → NewStream. For the mirror→primary direction that DROPPED
// the entire server-side JetStream stream (all messages) and recreated it
// empty at seq 0 — exactly the live activitylog data loss observed on the
// 2026-05-31 W→E flip (east activitylog seq1 timestamp == promote time, a
// fresh epoch; the pre-promote replicated history was gone). Detecting this
// direction lets the reconcile take the in-place UpdateConfiguration path
// instead, preserving the data.
func streamMirrorToPrimaryFlip(serverState *jsmapi.StreamConfig, spec *api.StreamSpec) bool {
	if serverState == nil || spec == nil {
		return false
	}
	return serverState.Mirror != nil && spec.Mirror == nil
}

// streamBackupGate evaluates whether the destructive recreate should be
// held off pending external backup confirmation. Returns:
//
//	gateFires == true  → caller must NOT destroy. Sets BackupRequired
//	                     condition with the returned reason+message.
//	gateFires == false → caller may proceed with delete + recreate.
//
// The gate ONLY fires when r.RequireBackupConfirmation() is true AND the
// local server stream has messages AND the cross-region peer can't
// confirm the data is replicated AND no fresh backup-confirmed
// annotation matches the CR's current generation. Any other state lets
// the caller proceed (preserves existing behavior).
//
// `gateErr` is returned only when the local state lookup itself errors
// out — caller should treat that as a hard reconcile failure.
func (r *StreamReconciler) streamBackupGate(jsm *jsm.Manager, stream *api.Stream, serverConfig *jsmapi.StreamConfig) (gateFires bool, reason, message string, gateErr error) {
	if !r.RequireBackupConfirmation() {
		return false, "", "", nil
	}
	// Inspect the live server state to learn the message count. The
	// config alone doesn't carry it.
	streamName := stream.Spec.Name
	s, err := jsm.LoadStream(streamName)
	if err != nil {
		if jsmapi.IsNatsErr(err, JSStreamNotFoundErr) {
			// No server stream yet — nothing to back up.
			return false, "", "", nil
		}
		return false, "", "", fmt.Errorf("load stream for state: %w", err)
	}
	state, err := s.LatestState()
	if err != nil {
		return false, "", "", fmt.Errorf("read stream state: %w", err)
	}
	if state.Msgs == 0 {
		// No local data to lose — safe to recreate.
		return false, "", "", nil
	}
	// Annotation gate: if drp-operator (or any external) signalled
	// that the backup is complete for THIS generation, proceed.
	if backupConfirmedForGeneration(stream.Annotations, r.BackupConfirmedAnnotation(), stream.Generation) {
		return false, "", "", nil
	}
	// Cross-region probe: when configured, fetch the peer's message
	// count for the same stream name and let it through if the peer
	// has at least as many messages locally.
	if url := r.CrossRegionNATSURL(); url != "" {
		peerMsgs, exists, probeErr := probeCrossRegionStreamMsgs(url, r.CrossRegionNATSCredsPath(), streamName, 5*time.Second)
		if probeErr == nil && exists && peerMsgs >= state.Msgs {
			return false, "", "", nil
		}
		switch {
		case probeErr != nil:
			return true, "PeerUnreachable",
				fmt.Sprintf("local has %d messages; cross-region probe failed: %v", state.Msgs, probeErr), nil
		case !exists:
			return true, "PeerMissing",
				fmt.Sprintf("local has %d messages; cross-region peer stream %q not found", state.Msgs, streamName), nil
		default:
			return true, "PeerStale",
				fmt.Sprintf("local has %d messages; cross-region peer has %d", state.Msgs, peerMsgs), nil
		}
	}
	return true, "NoProbeConfigured",
		fmt.Sprintf("local has %d messages and --cross-region-nats-url is unset", state.Msgs), nil
}

// isMirrorIncompatibleErr returns true for NATS API errors emitted when an
// in-place stream update would require flipping between source-mode and
// mirror-mode (10031 / 10034 / 10055).
func isMirrorIncompatibleErr(err error) bool {
	if err == nil {
		return false
	}
	return jsmapi.IsNatsErr(err, JSStreamMirrorInvalidErr) ||
		jsmapi.IsNatsErr(err, JSStreamMirrorWithSubjectsErr) ||
		jsmapi.IsNatsErr(err, JSStreamMirrorWithSourcesErr)
}

// isSubjectOverlapErr returns true for the NATS subject-overlap rejection
// (10065). During a mirror→primary promote this means the source still owns
// the subjects the destination is claiming (the demote has not converged yet).
// It is a TRANSIENT, RETRYABLE condition and MUST NOT trigger a destructive
// delete+recreate — see JSStreamSubjectOverlapErr's doc. Shared by the Stream
// and KeyValue reconcilers.
func isSubjectOverlapErr(err error) bool {
	if err == nil {
		return false
	}
	return jsmapi.IsNatsErr(err, JSStreamSubjectOverlapErr)
}
