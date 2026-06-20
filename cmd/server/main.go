package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alvor-technologies/iag-platform-go/authclient"
	platformotel "github.com/alvor-technologies/iag-platform-go/otel"

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
	"iag-traceability/backend/internal/scmclient"
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

	st := store.New(pool)
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
	consumerMetrics := metrics.New()
	if len(cfg.KafkaBrokers) > 0 {
		kc := consumer.New(consumer.Config{
			Brokers:          cfg.KafkaBrokers,
			GroupID:          cfg.KafkaConsumerGroup,
			SupplyChainTopic: cfg.KafkaSupplyChainTopic,
			ProductionTopic:  cfg.KafkaProductionTopic,
			QualityTopic:     cfg.KafkaQualityTopic,
		}, st, scm, consumerMetrics, kafkaPub)
		go func() {
			if err := kc.Run(ctx); err != nil {
				log.Printf("kafka consumer stopped: %v", err)
			}
		}()
	}

	api := &handlers.API{
		Cfg:             cfg,
		Store:           st,
		Audit:           auditStore,
		KafkaPub:        kafkaPub,
		QRCache:         qrCache,
		ConsumerMetrics: consumerMetrics,
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

func jwksRefreshLoop(ctx context.Context, v *authclient.Verifier) {
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			if err := v.Refresh(refreshCtx); err != nil {
				log.Printf("jwks refresh: %v", err)
			}
			cancel()
		}
	}
}
