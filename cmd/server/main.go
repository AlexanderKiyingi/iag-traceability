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

	"iag-traceability/backend/internal/cache"
	"iag-traceability/backend/internal/auditlog"
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
	ctx := context.Background()
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
	story.SetOptions(story.Options{PlaceholderJourney: cfg.StoryPlaceholderJourney})

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
		initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := verifier.Refresh(initCtx); err != nil {
			cancel()
			log.Fatalf("jwks refresh: %v", err)
		}
		cancel()
		go jwksRefreshLoop(verifier)
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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func jwksRefreshLoop(v *authclient.Verifier) {
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := v.Refresh(ctx); err != nil {
			log.Printf("jwks refresh: %v", err)
		}
		cancel()
	}
}
