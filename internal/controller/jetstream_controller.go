package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/jsm.go"
	js "github.com/nats-io/nack/controllers/jetstream"
	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	JSConsumerNotFoundErr uint16 = 10014
	JSStreamNotFoundErr   uint16 = 10059

	// Errors emitted by the NATS server when an in-place UpdateConfiguration
	// would have to flip a Stream between source-mode and mirror-mode. NATS
	// forbids this on an existing stream; recovery requires deleting the
	// server-side stream and re-creating it from the desired spec.
	JSStreamMirrorWithSourcesErr  uint16 = 10031
	JSStreamMirrorWithSubjectsErr uint16 = 10034
	JSStreamMirrorInvalidErr      uint16 = 10055

	// JSStreamSubjectOverlapErr is returned when a stream is configured with
	// subjects that overlap a DIFFERENT existing stream. During a DRP promote
	// (mirror→primary in place), the destination stream claims the subjects
	// the source previously owned. If the destination's UpdateConfiguration
	// lands BEFORE the source has released those subjects (i.e. before the
	// source has been demoted to mirror form server-side), the server rejects
	// the in-place promote with this code.
	//
	// CRITICAL: this is a TRANSIENT ORDERING condition, NOT a mirror-flip
	// incompatibility. It MUST NOT be treated as a trigger for the destructive
	// delete+recreate fallback — doing so would destroy exactly the messages
	// the in-place promote is meant to preserve. The correct handling is to
	// surface it as a retryable reconcile error so the next reconcile re-tries
	// the in-place update once the source has released the subjects. The DRP
	// flip runs DemotingSource — which converts the source to mirror form and
	// releases its subjects — BEFORE PromotingDestination, so in the real flip
	// the overlap clears on its own. Empirically reproduced + verified against
	// nats-server v2.14.0.
	JSStreamSubjectOverlapErr uint16 = 10065
)

var semVerRe = regexp.MustCompile(`\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?`)

type JetStreamController interface {
	client.Client

	// ReadOnly returns true when no changes should be made by the controller.
	ReadOnly() bool

	// ValidNamespace ok if the controllers namespace restriction allows the given namespace.
	ValidNamespace(namespace string) bool

	// WithJetStreamClient provides a jetStream client to the given operation.
	// The client uses the controllers connection configuration merged with opts.
	//
	// The given opts values take precedence over the controllers base configuration.
	//
	// Returns the error of the operation or errors during client setup.
	WithJetStreamClient(opts api.ConnectionOpts, ns string, op func(js jetstream.JetStream) error) error

	// WithJSMClient provides a jsm.go client to the given operation.
	WithJSMClient(opts api.ConnectionOpts, ns string, op func(jsm *jsm.Manager) error) error

	RequeueInterval() time.Duration

	// MirrorRecreateOnConflict returns true when the controller should force-
	// delete a Stream / KeyValue underlying stream and re-create it from the
	// K8s CR when a source<->mirror flip is detected, instead of letting
	// UpdateConfiguration silently retry forever.
	MirrorRecreateOnConflict() bool

	// RequireBackupConfirmation returns true when destructive recreate
	// should be gated on an external backup operator confirming the
	// local data via the BackupConfirmedAnnotation matching the CR's
	// current generation. See Config.RequireBackupConfirmation for the
	// full semantics.
	RequireBackupConfirmation() bool

	// CrossRegionNATSURL is the NATS URL the controller uses for the
	// pre-destruction "does the peer have this data?" probe. Empty
	// means the probe is skipped.
	CrossRegionNATSURL() string

	// CrossRegionNATSCredsPath is the local file path to the creds
	// used for the cross-region probe. Empty allowed.
	CrossRegionNATSCredsPath() string

	// BackupConfirmedAnnotation is the annotation key the controller
	// reads to know the external backup completed. Value must match
	// the CR's current generation (decimal) for the gate to clear.
	BackupConfirmedAnnotation() string

	// PassiveRoleTranslationEnabled returns true when the controller
	// should consult the `drp.cicada.io/local-role` namespace
	// annotation and translate primary-form CRs to mirror form at
	// the NATS-server level. See Config.EnablePassiveRoleTranslation.
	PassiveRoleTranslationEnabled() bool

	// CrossRegionNATSDomain is the peer region's JetStream domain
	// used when synthesizing mirror config under passive-role
	// translation. Empty disables translation.
	CrossRegionNATSDomain() string

	// ColdStartRoleDefaultsPassive returns true when an ABSENT
	// `drp.cicada.io/local-role` annotation should be treated as passive
	// (mirror) instead of the default active. Set on the secondary region
	// so a cold start fails closed. See Config.ColdStartRoleDefaultPassive.
	ColdStartRoleDefaultsPassive() bool
}

func NewJSController(k8sClient client.Client, natsConfig *NatsConfig, controllerConfig *Config) (JetStreamController, error) {
	return &jsController{
		Client:           k8sClient,
		config:           natsConfig,
		controllerConfig: controllerConfig,
		cacheDir:         controllerConfig.CacheDir,
		connPool:         newConnPool(time.Second * 15),
	}, nil
}

type jsController struct {
	client.Client
	config           *NatsConfig
	controllerConfig *Config
	cacheDir         string
	cacheLock        sync.Mutex
	connPool         *connectionPool
}

func (c *jsController) RequeueInterval() time.Duration {
	// Stagger the requeue slightly
	interval := c.controllerConfig.RequeueInterval

	// Allow up to a 10% variance
	intervalRange := float64(interval.Nanoseconds()) * 0.1

	randomFactor := (rand.Float64() * 2) - 1.0

	return time.Duration(float64(interval.Nanoseconds()) + (intervalRange * randomFactor))
}

func (c *jsController) ReadOnly() bool {
	return c.controllerConfig.ReadOnly
}

func (c *jsController) MirrorRecreateOnConflict() bool {
	return c.controllerConfig.MirrorRecreateOnConflict
}

func (c *jsController) RequireBackupConfirmation() bool {
	return c.controllerConfig.RequireBackupConfirmation
}

func (c *jsController) CrossRegionNATSURL() string {
	return c.controllerConfig.CrossRegionNATSURL
}

func (c *jsController) CrossRegionNATSCredsPath() string {
	return c.controllerConfig.CrossRegionNATSCredsPath
}

func (c *jsController) BackupConfirmedAnnotation() string {
	if c.controllerConfig.BackupConfirmedAnnotation == "" {
		return defaultBackupConfirmedAnnotation
	}
	return c.controllerConfig.BackupConfirmedAnnotation
}

func (c *jsController) PassiveRoleTranslationEnabled() bool {
	return c.controllerConfig.EnablePassiveRoleTranslation
}

func (c *jsController) CrossRegionNATSDomain() string {
	return c.controllerConfig.CrossRegionNATSDomain
}

func (c *jsController) ColdStartRoleDefaultsPassive() bool {
	return c.controllerConfig.ColdStartRoleDefaultPassive
}

func (c *jsController) ValidNamespace(namespace string) bool {
	ns := c.controllerConfig.Namespace
	return ns == "" || ns == namespace
}

func (c *jsController) WithJSMClient(opts api.ConnectionOpts, ns string, op func(js *jsm.Manager) error) error {
	cfg, err := c.natsConfigFromOpts(opts, ns)
	if err != nil {
		return err
	}

	conn, err := c.connPool.Get(cfg, true)
	if err != nil {
		return err
	}

	jsmClient, err := CreateJSMClient(conn, true, cfg.JsDomain)
	if err != nil {
		return fmt.Errorf("create jsm client: %w", err)
	}
	defer conn.Close()

	return op(jsmClient)
}

func (c *jsController) WithJetStreamClient(opts api.ConnectionOpts, ns string, op func(js jetstream.JetStream) error) error {
	cfg, err := c.natsConfigFromOpts(opts, ns)
	if err != nil {
		return err
	}

	conn, err := c.connPool.Get(cfg, true)
	if err != nil {
		return err
	}

	jsClient, err := CreateJetStreamClient(conn, true, cfg.JsDomain)
	if err != nil {
		return fmt.Errorf("create jetstream client: %w", err)
	}
	defer conn.Close()

	return op(jsClient)
}

// Setup default options, override from account resource and CRD options if configured
func (c *jsController) natsConfigFromOpts(opts api.ConnectionOpts, ns string) (*NatsConfig, error) {
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()

	natsConfig := &NatsConfig{}
	natsConfig.Overlay(c.config)

	if opts.Account == "" {
		natsConfig.Overlay(natsConfigFromOpts(opts))
		return natsConfig, nil
	}

	// Apply Account options first, over global.
	// Apply remaining CRD options last

	accountOverlay := &NatsConfig{}

	account := &api.Account{}
	err := c.Get(ctx,
		types.NamespacedName{
			Name:      opts.Account,
			Namespace: ns,
		},
		account,
	)
	if err != nil {
		return nil, err
	}

	if len(account.Spec.Servers) > 0 {
		accountOverlay.ServerURL = strings.Join(account.Spec.Servers, ",")
	}

	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	if account.Spec.TLS != nil && account.Spec.TLS.Secret != nil {
		tlsSecret := &v1.Secret{}
		err := c.Get(ctx,
			types.NamespacedName{
				Name:      account.Spec.TLS.Secret.Name,
				Namespace: ns,
			},
			tlsSecret,
		)
		if err != nil {
			return nil, err
		}

		accDir := filepath.Join(c.cacheDir, ns, opts.Account)
		if err := os.MkdirAll(accDir, 0o755); err != nil {
			return nil, err
		}

		var certData, keyData []byte
		var certPath, keyPath string

		for k, v := range tlsSecret.Data {
			switch k {
			case account.Spec.TLS.ClientCert:
				certPath = filepath.Join(accDir, k)
				certData = v
			case account.Spec.TLS.ClientKey:
				keyPath = filepath.Join(accDir, k)
				keyData = v
			case account.Spec.TLS.RootCAs:
				rootCAPath := filepath.Join(accDir, k)
				accountOverlay.CAs = append(accountOverlay.CAs, rootCAPath)

				if _, err := os.Stat(rootCAPath); err == nil {
					caBytes, err := os.ReadFile(rootCAPath)
					// Skip file write if data is unchanged
					if err == nil && bytes.Equal(caBytes, v) {
						continue
					}
				}

				if err := os.WriteFile(rootCAPath, v, 0o644); err != nil {
					return nil, err
				}
			}
		}

		if certData != nil && keyData != nil {
			accountOverlay.Certificate = certPath
			accountOverlay.Key = keyPath

			writeCert := true
			if _, err := os.Stat(certPath); err == nil {
				fileBytes, err := os.ReadFile(certPath)
				// Skip disk write if data is unchanged
				if err == nil && bytes.Equal(fileBytes, certData) {
					writeCert = false
				}
			}

			if writeCert {
				if err := os.WriteFile(certPath, certData, 0o644); err != nil {
					return nil, err
				}
			}

			writeKey := true
			if _, err := os.Stat(keyPath); err == nil {
				fileBytes, err := os.ReadFile(keyPath)
				// Skip disk write if data is unchanged
				if err == nil && bytes.Equal(fileBytes, keyData) {
					writeKey = false
				}
			}

			if writeKey {
				if err := os.WriteFile(keyPath, keyData, 0o600); err != nil {
					return nil, err
				}
			}
		}
	} else if account.Spec.TLS != nil {
		if account.Spec.TLS.ClientCert != "" && account.Spec.TLS.ClientKey != "" {
			accountOverlay.Certificate = account.Spec.TLS.ClientCert
			accountOverlay.Key = account.Spec.TLS.ClientKey
		}
		accountOverlay.CAs = []string{account.Spec.TLS.RootCAs}
	}

	if account.Spec.Creds != nil && account.Spec.Creds.Secret != nil {
		credsSecret := &v1.Secret{}
		err := c.Get(ctx,
			types.NamespacedName{
				Name:      account.Spec.Creds.Secret.Name,
				Namespace: ns,
			},
			credsSecret,
		)
		if err != nil {
			return nil, err
		}

		accDir := filepath.Join(c.cacheDir, ns, opts.Account)
		if err := os.MkdirAll(accDir, 0o755); err != nil {
			return nil, err
		}

		if credsBytes, ok := credsSecret.Data[account.Spec.Creds.File]; ok {
			filePath := filepath.Join(accDir, account.Spec.Creds.File)
			accountOverlay.Credentials = filePath

			writeCreds := true
			if _, err := os.Stat(filePath); err == nil {
				fileBytes, err := os.ReadFile(filePath)
				// Skip disk write if data is unchanged
				if err == nil && bytes.Equal(fileBytes, credsBytes) {
					writeCreds = false
				}
			}

			if writeCreds {
				if err := os.WriteFile(filePath, credsBytes, 0o600); err != nil {
					return nil, err
				}
			}
		}
	} else if account.Spec.Creds != nil {
		accountOverlay.Credentials = account.Spec.Creds.File
	}

	if account.Spec.NKey != nil && account.Spec.NKey.Secret != nil {
		nkeySecret := &v1.Secret{}
		err := c.Get(ctx,
			types.NamespacedName{
				Name:      account.Spec.NKey.Secret.Name,
				Namespace: ns,
			},
			nkeySecret,
		)
		if err != nil {
			return nil, err
		}

		accDir := filepath.Join(c.cacheDir, ns, opts.Account)
		if err := os.MkdirAll(accDir, 0o755); err != nil {
			return nil, err
		}

		if nkeyBytes, ok := nkeySecret.Data[account.Spec.NKey.Seed]; ok {
			filePath := filepath.Join(accDir, account.Spec.NKey.Seed)
			accountOverlay.NKey = filePath

			writeNKey := true
			if _, err := os.Stat(filePath); err == nil {
				fileBytes, err := os.ReadFile(filePath)
				if err == nil && bytes.Equal(fileBytes, nkeyBytes) {
					writeNKey = false
				}
			}

			if writeNKey {
				if err := os.WriteFile(filePath, nkeyBytes, 0o600); err != nil {
					return nil, err
				}
			}
		}
	}

	if account.Spec.User != nil {
		userSecret := &v1.Secret{}
		err := c.Get(ctx,
			types.NamespacedName{
				Name:      account.Spec.User.Secret.Name,
				Namespace: ns,
			},
			userSecret,
		)
		if err != nil {
			return nil, err
		}

		userName := userSecret.Data[account.Spec.User.User]
		userPassword := userSecret.Data[account.Spec.User.Password]

		if userName != nil && userPassword != nil {
			accountOverlay.User = string(userName)
			accountOverlay.Password = string(userPassword)
		}
	}

	if account.Spec.Token != nil {
		tokenSecret := &v1.Secret{}
		err := c.Get(ctx,
			types.NamespacedName{
				Name:      account.Spec.Token.Secret.Name,
				Namespace: ns,
			},
			tokenSecret,
		)
		if err != nil {
			return nil, err
		}

		if token := tokenSecret.Data[account.Spec.Token.Token]; token != nil {
			accountOverlay.Token = string(token)
		}
	}

	// Overlay Account Config
	natsConfig.Overlay(accountOverlay)

	// Overlay Spec Config
	natsConfig.Overlay(natsConfigFromOpts(opts))

	return natsConfig, nil
}

func natsConfigFromOpts(opts api.ConnectionOpts) *NatsConfig {
	natsConfig := &NatsConfig{}

	if len(opts.Servers) > 0 {
		natsConfig.ServerURL = strings.Join(opts.Servers, ",")
	}

	// Currently, if the global TLSFirst is set, a false value in the CRD will not override
	// due to that being the bool zero value. A true value in the CRD can override a global false.
	if opts.TLSFirst {
		natsConfig.TLSFirst = opts.TLSFirst
	}

	if opts.Creds != "" {
		natsConfig.Credentials = opts.Creds
	}

	if opts.Nkey != "" {
		natsConfig.NKey = opts.Nkey
	}

	if opts.TLS != nil {
		if len(opts.TLS.RootCAs) > 0 {
			natsConfig.CAs = opts.TLS.RootCAs
		}

		if opts.TLS.ClientCert != "" && opts.TLS.ClientKey != "" {
			natsConfig.Certificate = opts.TLS.ClientCert
			natsConfig.Key = opts.TLS.ClientKey
		}
	}

	if opts.JsDomain != "" {
		natsConfig.JsDomain = opts.JsDomain
	}

	return natsConfig
}

// updateReadyCondition returns the given conditions with an added or updated ready condition.
func updateReadyCondition(conditions []api.Condition, status v1.ConditionStatus, reason string, message string) []api.Condition {
	var currentStatus v1.ConditionStatus
	var lastTransitionTime string
	for _, condition := range conditions {
		if condition.Type == readyCondType {
			currentStatus = condition.Status
			lastTransitionTime = condition.LastTransitionTime
			break
		}
	}

	// Set transition time to now, when no previous ready condition or the status changed
	if lastTransitionTime == "" || currentStatus != status {
		lastTransitionTime = time.Now().UTC().Format(time.RFC3339Nano)
	}

	newCondition := api.Condition{
		Type:               readyCondType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: lastTransitionTime,
	}
	if conditions == nil {
		return []api.Condition{newCondition}
	} else {
		return js.UpsertCondition(conditions, newCondition)
	}
}

// jsonString returns the given string wrapped in " and converted to []byte.
// Helper for mapping spec config to jetStream config using UnmarshalJSON.
func jsonString(v string) []byte {
	return []byte("\"" + v + "\"")
}

// updateBackupRequiredCondition upserts the BackupRequired condition.
// Pass v1.ConditionFalse + empty reason/message to clear the gate.
func updateBackupRequiredCondition(conditions []api.Condition, status v1.ConditionStatus, reason string, message string) []api.Condition {
	var currentStatus v1.ConditionStatus
	var lastTransitionTime string
	for _, condition := range conditions {
		if condition.Type == conditionBackupRequired {
			currentStatus = condition.Status
			lastTransitionTime = condition.LastTransitionTime
			break
		}
	}
	if lastTransitionTime == "" || currentStatus != status {
		lastTransitionTime = time.Now().UTC().Format(time.RFC3339Nano)
	}
	newCondition := api.Condition{
		Type:               conditionBackupRequired,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: lastTransitionTime,
	}
	if conditions == nil {
		return []api.Condition{newCondition}
	}
	return js.UpsertCondition(conditions, newCondition)
}

// removeBackupRequiredCondition strips the BackupRequired condition entry
// entirely from the slice. Use after a successful destructive recreate
// so the CR's `.status.conditions` doesn't carry the stale gate marker.
func removeBackupRequiredCondition(conditions []api.Condition) []api.Condition {
	out := conditions[:0]
	for _, c := range conditions {
		if c.Type == conditionBackupRequired {
			continue
		}
		out = append(out, c)
	}
	return out
}

// updatePassiveRoleTranslatedCondition upserts the PassiveRoleTranslated
// condition. Status=True when the most recent reconcile rewrote the CR's
// spec to mirror form at the NATS-server level (CR untouched). Reason
// carries the source domain so an operator can verify which peer the
// translation pointed at.
func updatePassiveRoleTranslatedCondition(conditions []api.Condition, status v1.ConditionStatus, reason string, message string) []api.Condition {
	var currentStatus v1.ConditionStatus
	var lastTransitionTime string
	for _, condition := range conditions {
		if condition.Type == conditionPassiveRoleTranslated {
			currentStatus = condition.Status
			lastTransitionTime = condition.LastTransitionTime
			break
		}
	}
	if lastTransitionTime == "" || currentStatus != status {
		lastTransitionTime = time.Now().UTC().Format(time.RFC3339Nano)
	}
	newCondition := api.Condition{
		Type:               conditionPassiveRoleTranslated,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: lastTransitionTime,
	}
	if conditions == nil {
		return []api.Condition{newCondition}
	}
	return js.UpsertCondition(conditions, newCondition)
}

// removePassiveRoleTranslatedCondition strips the PassiveRoleTranslated
// condition. Use after a reconcile pass that did NOT translate (e.g.
// namespace became active, or the feature was disabled).
func removePassiveRoleTranslatedCondition(conditions []api.Condition) []api.Condition {
	out := conditions[:0]
	for _, c := range conditions {
		if c.Type == conditionPassiveRoleTranslated {
			continue
		}
		out = append(out, c)
	}
	return out
}

// backupConfirmedForGeneration reports whether the CR's
// `metadata.annotations[<annotationKey>]` equals the decimal form of
// the expected generation. Used as the clear-path predicate for the
// BackupRequired gate.
func backupConfirmedForGeneration(annotations map[string]string, annotationKey string, expectedGeneration int64) bool {
	if annotations == nil {
		return false
	}
	v, ok := annotations[annotationKey]
	if !ok || v == "" {
		return false
	}
	return v == strconv.FormatInt(expectedGeneration, 10)
}

// probeCrossRegionStreamMsgs opens a short-lived NATS connection to
// `serverURL`, queries `streamName` JetStream stream info, and returns
// the message count + a stream-not-found flag. Returns (0, false, err)
// on any connection/auth failure so the gate can demand a backup in
// the "can't confirm" case.
func probeCrossRegionStreamMsgs(serverURL, credsPath, streamName string, timeout time.Duration) (msgs uint64, exists bool, err error) {
	opts := []nats.Option{
		nats.Name("nack-cross-region-probe"),
		nats.Timeout(timeout),
		nats.MaxReconnects(0),
		nats.RetryOnFailedConnect(false),
	}
	if credsPath != "" {
		opts = append(opts, nats.UserCredentials(credsPath))
	}
	nc, err := nats.Connect(serverURL, opts...)
	if err != nil {
		return 0, false, fmt.Errorf("connect cross-region NATS %s: %w", serverURL, err)
	}
	defer nc.Close()

	jsCtx, err := jetstream.New(nc)
	if err != nil {
		return 0, false, fmt.Errorf("jetstream client: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	info, err := jsCtx.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("stream info %s: %w", streamName, err)
	}
	return info.CachedInfo().State.Msgs, true, nil
}

func compareConfigState(actual any, desired any) string {
	return cmp.Diff(desired, actual)
}

func versionComponents(version string) (major, minor, patch int, err error) {
	m := semVerRe.FindStringSubmatch(version)
	if m == nil {
		return 0, 0, 0, errors.New("invalid semver")
	}
	major, err = strconv.Atoi(m[1])
	if err != nil {
		return -1, -1, -1, err
	}
	minor, err = strconv.Atoi(m[2])
	if err != nil {
		return -1, -1, -1, err
	}
	patch, err = strconv.Atoi(m[3])
	if err != nil {
		return -1, -1, -1, err
	}
	return major, minor, patch, err
}
