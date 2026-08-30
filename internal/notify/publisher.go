// Package notify gives traceability a way to raise a user-visible
// notification. The service had none: it consumed from three topics and
// published domain events, but nothing it detected could ever reach a person
// by email.
//
// It publishes notification.requested on iag.notifications, the same path
// iag-erp and iag-finance use, so no new consumer or topic is needed.
package notify

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	platformevents "github.com/alvor-technologies/iag-platform-go/events"
)

const (
	Source                    = "iag.traceability"
	TypeNotificationRequested = "notification.requested"
	TopicNotifications        = "iag.notifications"

	// TemplateAlert is the generic Title/Body alert template. Seeded centrally
	// alongside the other <svc>.alert templates.
	TemplateAlert = "traceability.alert"

	// AudienceOps is the desk told about operational faults. An administrator
	// routes it in the notifications admin UI; until then DefaultRecipient is
	// the fallback.
	AudienceOps = "ops.traceability"
)

// Publisher raises notifications. The zero value is a no-op, so a deployment
// without Kafka simply sends nothing rather than failing.
type Publisher struct {
	producer *platformevents.Producer
	topic    string

	mu   sync.Mutex
	last map[string]time.Time
}

type Config struct {
	Brokers  []string
	ClientID string
	Topic    string
}

func New(cfg Config) *Publisher {
	if len(cfg.Brokers) == 0 || (len(cfg.Brokers) == 1 && strings.TrimSpace(cfg.Brokers[0]) == "") {
		return &Publisher{}
	}
	topic := cfg.Topic
	if topic == "" {
		topic = TopicNotifications
	}
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = Source
	}
	return &Publisher{
		producer: platformevents.NewProducer(platformevents.ProducerConfig{
			Brokers:  cfg.Brokers,
			ClientID: clientID,
		}),
		topic: topic,
		last:  make(map[string]time.Time),
	}
}

func (p *Publisher) Enabled() bool { return p != nil && p.producer != nil }

func (p *Publisher) Close() error {
	if p == nil || p.producer == nil {
		return nil
	}
	return p.producer.Close()
}

// DefaultRecipient is the fallback address, matching the NOTIFY_DEFAULT_RECIPIENT
// convention every other alert emitter on the platform follows.
func DefaultRecipient() string {
	return strings.TrimSpace(os.Getenv("NOTIFY_DEFAULT_RECIPIENT"))
}

// AlertOnce raises an alert at most once per window for a given key.
//
// The throttle is the point. The condition this exists for — the compliance
// gate being unreachable — fails on *every* publish attempt while it lasts, so
// an unthrottled alert would mail the ops desk once per request during exactly
// the incident when the desk needs to be able to read its mail.
func (p *Publisher) AlertOnce(ctx context.Context, key, title, body string, window time.Duration) {
	if !p.Enabled() {
		return
	}
	if !p.allow(key, window) {
		return
	}
	p.Alert(ctx, key, title, body)
}

func (p *Publisher) allow(key string, window time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if last, ok := p.last[key]; ok && now.Sub(last) < window {
		return false
	}
	p.last[key] = now
	return true
}

// Alert raises one alert addressed to the ops audience, with the configured
// default recipient as the fallback until that audience is routed.
func (p *Publisher) Alert(ctx context.Context, key, title, body string) {
	if !p.Enabled() {
		return
	}
	recipient := DefaultRecipient()
	if recipient == "" {
		// An audience with no route and no fallback reaches nobody. Say so
		// once rather than emitting events into the void.
		warnNoRecipient()
	}
	env := platformevents.NewEnvelope(Source, TypeNotificationRequested, map[string]any{
		"channel":    "email",
		"recipient":  recipient,
		"audience":   AudienceOps,
		"templateId": TemplateAlert,
		"variables": map[string]any{
			"Title": title,
			"Body":  body,
		},
	})
	if err := p.producer.Publish(ctx, p.topic, key, env); err != nil {
		slog.WarnContext(ctx, "traceability alert publish failed", "key", key, "err", err)
	}
}

var noRecipientOnce sync.Once

func warnNoRecipient() {
	noRecipientOnce.Do(func() {
		slog.Warn("traceability alerts have no fallback recipient: set NOTIFY_DEFAULT_RECIPIENT, " +
			"or route the " + AudienceOps + " audience in the notifications admin")
	})
}
