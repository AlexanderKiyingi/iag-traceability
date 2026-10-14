package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alvor-technologies/iag-platform-go/authclient"
	platformotel "github.com/alvor-technologies/iag-platform-go/otel"
	"github.com/jackc/pgx/v5/pgxpool"

	"iag-traceability/backend/internal/anchor"
	"iag-traceability/backend/internal/auditlog"
	"iag-traceability/backend/internal/cache"
	"iag-traceability/backend/internal/config"
	"iag-traceability/backend/internal/consumer"
	"iag-traceability/backend/internal/db"
	"iag-traceability/backend/internal/handlers"
	"iag-traceability/backend/internal/kafkabus"
	"iag-traceability/backend/internal/metrics"
	"iag-traceability/backend/internal/middleware"
	"iag-traceability/backend/internal/migrate"
	"iag-traceability/backend/internal/notify"
	"iag-traceability/backend/internal/scmclient"
	"iag-traceability/backend/internal/scmsync"
	"iag-traceability/backend/internal/store"
	"iag-traceability/backend/internal/story"
)

func main() {
	// Root context for background workers (Kafka consumer, refresh loops). Cancelled
	// on SIGTERM so in-flight work can drain rather than being killed abruptly.
	ctx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// OpenTelemetry tracing → otel-collector:4317 (non-blocking dial, so a
	// missing/late collector never blocks boot). Degrade to a no-op tracer on
	// error rather than failing startup; tracing is observability, not
	// correctness. Brings traceability in line with the platform-wide OTel
	// uniformity (every other service initializes this).
	if tp, oerr := platformotel.Init(ctx, platformotel.Config{
		ServiceName: "iag-traceability",
		Environment: cfg.Environment,
	}); oerr != nil {
		log.Printf("traceability: otel disabled (%v)", oerr)
	} else {
		defer func() {
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tp.Shutdown(shutCtx)
		}()
	}

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer pool.Close()

	if cfg.AutoMigrate {
		if err := migrate.Up(ctx, pool); err != nil {
			log.Fatalf("migrate: %v", err)
		}
	}

	st := store.New(pool).SetChainEnabled(cfg.ChainEnabled)
	if cfg.ChainEnabled {
		log.Printf("[traceability] custody hash-chain ENABLED")
	}
	auditStore := auditlog.NewStore(pool)

	scm := scmclient.New(
		cfg.SupplyChainBaseURL, cfg.AuthTokenURL,
		cfg.ServiceClientID, cfg.ServiceClientSecret, cfg.SupplyChainAudience,
	)
	story.SetSCMClient(scm)
	story.SetOptions(story.Options{
		PlaceholderJourney: cfg.StoryPlaceholderJourney,
		PreciseGeo:         cfg.PublicPreciseGeo,
	})

	var qrCache *cache.JSONCache
	if cfg.RedisURL != "" {
		if rdb, err := cache.NewClient(cfg.RedisURL); err != nil {
			log.Printf("traceability: redis unavailable (%v) — public QR cache disabled", err)
		} else {
			qrCache = cache.NewJSONCache(rdb, cfg.CacheTTL)
		}
	}

	var verifier *authclient.Verifier
	if cfg.AuthMode == "jwt" {
		verifier = authclient.NewVerifier(authclient.Options{
			JWKSURL:  cfg.JWKSURL,
			Issuer:   cfg.JWTIssuer,
			Audience: cfg.Audience,
		})
		// Bounded retry, then degrade-to-background. A transient JWKS failure at
		// boot (auth service restarting, DNS blip) must NOT crash the process —
		// that turns a brief upstream hiccup into a Railway crash-loop. If the
		// initial refresh never succeeds, the background loop keeps trying and
		// auth requests fail closed (401) until keys are available.
		if err := refreshJWKSWithRetry(ctx, verifier); err != nil {
			log.Printf("traceability: jwks not ready at boot (%v) — continuing; background refresh will retry, auth fails closed until keys load", err)
		}
		go jwksRefreshLoop(ctx, verifier)
	}

	platformAuth := middleware.NewPlatformAuth(middleware.PlatformAuthOptions{
		Mode:     cfg.AuthMode,
		Verifier: verifier,
	})

	if cfg.AuthMode == "jwt" && cfg.ServiceClientSecret != "" {
		go registerPermissionsLoop(ctx, cfg)
	} else if cfg.AuthMode == "jwt" {
		log.Printf("traceability: SERVICE_CLIENT_SECRET unset — skipping permissions registration")
	}

	kafkaPub := kafkabus.NewPublisher(cfg.KafkaBrokers, cfg.KafkaClientID)

	// Operational alerts by email. Publishes notification.requested on
	// iag.notifications, so it needs no consumer of its own; a no-op when
	// Kafka is not configured.
	notifier := notify.New(notify.Config{Brokers: cfg.KafkaBrokers, ClientID: cfg.KafkaClientID})
	defer func() { _ = notifier.Close() }()
	consumerMetrics := metrics.New()
	if len(cfg.KafkaBrokers) > 0 {
		kc := consumer.New(consumer.Config{
			Brokers:          cfg.KafkaBrokers,
			GroupID:          cfg.KafkaConsumerGroup,
			SupplyChainTopic: cfg.KafkaSupplyChainTopic,
			ProductionTopic:  cfg.KafkaProductionTopic,
			QualityTopic:     cfg.KafkaQualityTopic,
			// Story-updated events from projections are enqueued to the
			// transactional outbox and delivered by the relay below.
		}, st, scm, consumerMetrics, kafkabus.NewOutboxEmitter(st))
		go func() {
			if err := kc.Run(ctx); err != nil {
				log.Printf("kafka consumer stopped: %v", err)
			}
		}()

		// Relay: drain the transactional outbox to Kafka. Only runs when a
		// broker is configured; otherwise events accumulate durably until it is.
		go kafkabus.NewRelay(st, kafkaPub).Run(ctx)
	}

	// SCM custody sync: copy SCM's public.trace_events / lot_qr_codes in, since
	// nothing relays SCM's outbox and service auth is refused. Errors are
	// logged and retried each tick; they never stop the server.
	if cfg.SCMSyncEnabled {
		startSCMSync(ctx, cfg, pool, st)
	}

	// L2 notarization / L3 anchoring. Opt-in (ANCHOR_ENABLED): a signer attests a
	// Merkle root of custody blocks, an optional provider posts it on-chain. A
	// misconfigured signer/provider is fatal at boot — silently degrading to an
	// unsigned "notarization" would be worse than not offering it.
	var anchorSvc *anchor.Service
	if cfg.AnchorEnabled {
		signer, err := anchor.NewSigner(anchor.SignerConfig{
			Algo:       cfg.AnchorSigningAlgo,
			KeyID:      cfg.AnchorKeyID,
			KeyHex:     cfg.AnchorSigningKey,
			VaultAddr:  cfg.VaultAddr,
			VaultToken: cfg.VaultToken,
			VaultMount: cfg.VaultTransitMount,
			VaultKey:   cfg.VaultTransitKey,
		})
		if err != nil {
			log.Fatalf("anchor signer: %v", err)
		}
		provider, err := anchor.NewProvider(cfg.AnchorProvider, cfg.AnchorRPCURL, cfg.AnchorContract)
		if err != nil {
			log.Fatalf("anchor provider: %v", err)
		}
		anchorSvc = anchor.NewService(st, signer, provider)
		log.Printf("[traceability] notarization ENABLED (algo=%s provider=%s)", signer.Algo(), provider.Name())
		if cfg.AnchorInterval > 0 {
			go anchorJobLoop(ctx, anchorSvc, cfg.AnchorInterval)
		}
	}

	api := &handlers.API{
		Cfg:             cfg,
		Store:           st,
		Audit:           auditStore,
		KafkaPub:        kafkaPub,
		QRCache:         qrCache,
		ConsumerMetrics: consumerMetrics,
		Anchor:          anchorSvc,
	}
	if !cfg.LocalEnvironment() && !cfg.PublicTraceURLConfigured() {
		log.Printf("traceability: PUBLIC_TRACE_BASE_URL is %q — QR publishing is refused until it is set to the public trace address", cfg.PublicTraceBaseURL)
	}
	router := handlers.NewRouter(handlers.RouterDeps{
		API:              api,
		Audit:            auditStore,
		PlatformAuth:     platformAuth,
		CORSOrigins:      cfg.CORSOrigins,
		PublicRatePerMin: cfg.PublicRateLimitPerMin,
		PublicRateBurst:  cfg.PublicRateBurst,
		TrustedProxies:   cfg.TrustedProxies,
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Printf("traceability listening on :%s (aud=%s)", cfg.Port, cfg.Audience)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	// Signal background workers (consumer, refresh loops) to drain, then stop
	// accepting new HTTP requests.
	cancelRoot()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// refreshJWKSWithRetry attempts the initial JWKS load with a few bounded retries
// so a slow-but-recovering auth service doesn't fail boot on the first attempt.
// Gives up (returns the last error) after the attempts are exhausted — the
// caller degrades to the background refresh loop rather than exiting.
func refreshJWKSWithRetry(ctx context.Context, v *authclient.Verifier) error {
	const attempts = 5
	var err error
	for i := 0; i < attempts; i++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = v.Refresh(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if i == attempts-1 {
			break // last attempt failed — don't sleep, let the caller degrade
		}
		backoff := time.Duration(i+1) * 2 * time.Second
		log.Printf("traceability: jwks refresh attempt %d/%d failed (%v) — retrying in %s", i+1, attempts, err, backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return err
}

// jwksRefreshLoop keeps the key set current at two speeds: the steady rotation
// interval once keys are loaded, and a much shorter one while the set is empty.
//
// A flat 15 minute tick was the problem: boot already tolerates a missing JWKS
// and fails closed until keys arrive, but if the auth service restarts and this
// service happens to hold nothing, every authenticated request is rejected for
// up to a quarter of an hour. Observed in the wild — a group of services 401'd
// together for a few minutes after an auth restart while those with a fast
// refresh never blipped.
func jwksRefreshLoop(ctx context.Context, v *authclient.Verifier) {
	const (
		steadyInterval   = 15 * time.Minute
		degradedInterval = 15 * time.Second
	)
	for {
		wait := steadyInterval
		if !v.HasKeys() {
			wait = degradedInterval
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		hadKeys := v.HasKeys()
		refreshCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := v.Refresh(refreshCtx)
		cancel()
		switch {
		case err != nil && hadKeys:
			// Previous key set is still in memory; tokens keep verifying.
			log.Printf("jwks refresh failed; serving with the previous key set: %v", err)
		case err != nil:
			log.Printf("jwks still unavailable; all authenticated requests are being rejected: %v", err)
		case !hadKeys:
			log.Printf("jwks recovered; token verification restored")
		}
	}
}

// anchorJobLoop periodically notarizes any custody blocks appended since the
// last anchor. ErrNothingToAnchor is the steady state and is not logged as an
// error; anything else is surfaced for ops monitoring.
func anchorJobLoop(ctx context.Context, svc *anchor.Service, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			jobCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			row, err := svc.CreateAnchor(jobCtx)
			cancel()
			switch {
			case err == nil:
				log.Printf("[anchor] notarized blocks %d..%d (%d events) root=%s", row.FromSeq, row.ToSeq, row.EventCount, row.MerkleRoot)
			case errors.Is(err, anchor.ErrNothingToAnchor):
				// steady state — nothing new to anchor
			default:
				log.Printf("[anchor] notarization job failed: %v", err)
			}
		}
	}
}

// startSCMSync wires the background SCM sync. A missing or unreachable SCM
// database is logged, not fatal.
func startSCMSync(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, st *store.Store) {
	source := pool
	switch {
	case cfg.SCMSyncSameDB:
		log.Printf("[scm-sync] ENABLED (same database, every %s)", cfg.SCMSyncInterval)
	case cfg.SCMDatabaseURL != "":
		scmPool, err := pgxpool.New(ctx, cfg.SCMDatabaseURL)
		if err != nil {
			log.Printf("[scm-sync] disabled: SCM_DATABASE_URL: %v", err)
			return
		}
		source = scmPool
		go func() { <-ctx.Done(); scmPool.Close() }()
		log.Printf("[scm-sync] ENABLED (SCM_DATABASE_URL, every %s)", cfg.SCMSyncInterval)
	default:
		log.Printf("[scm-sync] disabled: SCM_SYNC_SAME_DB=false and SCM_DATABASE_URL is empty")
		return
	}
	go scmsync.New(source, st).Loop(ctx, cfg.SCMSyncInterval)
}
