package controller

// Unit tests for the namespace-watch predicate that drives instant
// re-reconcile on a local-role flip (local_role_watch.go).

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func nsWithRole(role string) *corev1.Namespace {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats"}}
	if role != "" {
		ns.Annotations = map[string]string{localRoleAnnotation: role}
	}
	return ns
}

func TestLocalRoleChangedPredicate(t *testing.T) {
	p := localRoleChangedPredicate()

	t.Run("create with role → enqueue", func(t *testing.T) {
		if !p.Create(event.CreateEvent{Object: nsWithRole("passive")}) {
			t.Error("create of a namespace carrying local-role should enqueue")
		}
	})
	t.Run("create without role → ignore", func(t *testing.T) {
		if p.Create(event.CreateEvent{Object: nsWithRole("")}) {
			t.Error("create of a namespace with no local-role should not enqueue")
		}
	})
	t.Run("update changing role → enqueue", func(t *testing.T) {
		if !p.Update(event.UpdateEvent{ObjectOld: nsWithRole("active"), ObjectNew: nsWithRole("passive")}) {
			t.Error("active→passive must enqueue")
		}
	})
	t.Run("update adding role (absent→passive) → enqueue", func(t *testing.T) {
		if !p.Update(event.UpdateEvent{ObjectOld: nsWithRole(""), ObjectNew: nsWithRole("passive")}) {
			t.Error("absent→passive must enqueue (cold-start stamp)")
		}
	})
	t.Run("update removing role (active→absent) → enqueue", func(t *testing.T) {
		if !p.Update(event.UpdateEvent{ObjectOld: nsWithRole("active"), ObjectNew: nsWithRole("")}) {
			t.Error("active→absent must enqueue (drift to correct)")
		}
	})
	t.Run("update with SAME role → ignore (no churn)", func(t *testing.T) {
		if p.Update(event.UpdateEvent{ObjectOld: nsWithRole("passive"), ObjectNew: nsWithRole("passive")}) {
			t.Error("unchanged local-role must NOT enqueue (avoids reconcile churn on unrelated ns updates)")
		}
	})
	t.Run("delete → ignore", func(t *testing.T) {
		if p.Delete(event.DeleteEvent{Object: nsWithRole("passive")}) {
			t.Error("namespace delete is not a role flip")
		}
	})
	t.Run("generic → ignore", func(t *testing.T) {
		if p.Generic(event.GenericEvent{Object: nsWithRole("passive")}) {
			t.Error("generic event must not enqueue")
		}
	})
}
