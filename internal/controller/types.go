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
)
