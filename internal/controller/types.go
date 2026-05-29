package controller

const (
	readyCondType        = "Ready"
	accountFinalizer     = "account.nats.io/finalizer"
	streamFinalizer      = "stream.nats.io/finalizer"
	keyValueFinalizer    = "kv.nats.io/finalizer"
	objectStoreFinalizer = "objectstore.nats.io/finalizer"
	consumerFinalizer    = "consumer.nats.io/finalizer"

	stateAnnotationConsumer = "consumer.nats.io/state"
	stateAnnotationKV       = "kv.nats.io/state"
	stateAnnotationObj      = "objectstore.nats.io/state"
	stateAnnotationStream   = "stream.nats.io/state"

	stateReady            = "Ready"
	stateReconciling      = "Reconciling"
	stateErrored          = "Errored"
	stateFinalizing       = "Finalizing"
	stateWaitingForBackup = "WaitingForBackup"

	// Condition type written when a destructive source<->mirror recreate
	// is gated on an external backup step (see drp-operator). The
	// reconciler will not proceed with the delete until the matching
	// backup-confirmed annotation is observed for the current generation.
	conditionBackupRequired = "BackupRequired"

	// Default annotation key the controller looks for to confirm that an
	// external backup operator has captured the local stream state. The
	// annotation value is the generation number for which the backup was
	// taken; only matches against the CR's current generation count.
	// Overrideable via --backup-confirmed-annotation.
	defaultBackupConfirmedAnnotation = "drp.cicada.io/backup-confirmed-generation"

	// Namespace-level annotation declaring the data-plane role this region
	// holds steady-state. The drp-operator stamps this on the `nats`
	// namespace post-flip:
	//
	//   passive = "I am the mirror side; ignore K8s CR Subjects and
	//             synthesize a mirror config from --cross-region-nats-domain
	//             + spec.name when applying to the NATS server."
	//   (any other value, or absent) = "primary semantics" — apply the CR
	//             as authored.
	//
	// The translation is server-side only: the K8s CR is NOT modified, so
	// ArgoCD selfHeal continues to see the chart's primary form and stays
	// Healthy. Pairs with --enable-passive-role-translation +
	// --cross-region-nats-domain (this flag is required for translation to
	// fire — without a remote domain the controller can't synthesize a
	// mirror externalApiPrefix).
	localRoleAnnotation = "drp.cicada.io/local-role"
	localRolePassive    = "passive"

	// Condition type set on Stream / KeyValue CRs when the passive-role
	// translation kicked in for the most recent reconcile pass. Distinct
	// from Ready — translation does not block readiness, it just records
	// that what was applied to NATS server differs from what the CR
	// authors. Cleared on reconciles where the namespace is not passive.
	conditionPassiveRoleTranslated = "PassiveRoleTranslated"
)
