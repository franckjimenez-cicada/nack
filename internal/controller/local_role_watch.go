package controller

// local_role_watch.go — shared bits for watching the `nats` namespace's
// drp.cicada.io/local-role annotation so the Stream/KeyValue controllers
// re-reconcile their CRs the INSTANT the role flips, instead of waiting
// for the next resync (~minutes). Without this, a role change that is NOT
// accompanied by a CR change (e.g. the drp-operator's steady-state
// local-role loop correcting drift) is only picked up on the next periodic
// reconcile. The flip path mutates the CRs and so already re-triggers, but
// a bare annotation change does not — this closes that latency gap.

import (
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// localRoleChangedPredicate fires only for namespace events that actually
// change the drp.cicada.io/local-role annotation — so the Stream/KeyValue
// controllers don't re-enqueue every CR on unrelated namespace updates
// (label churn, other annotations, resourceVersion bumps).
//
//   - Create:  enqueue (a namespace appearing with a role set should be
//     reflected; harmless if no CRs exist yet).
//   - Update:  enqueue ONLY when the local-role value differs old→new.
//   - Delete/Generic: ignore (namespace teardown is not a role flip).
func localRoleChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object.GetAnnotations()[localRoleAnnotation] != ""
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetAnnotations()[localRoleAnnotation] !=
				e.ObjectNew.GetAnnotations()[localRoleAnnotation]
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}
