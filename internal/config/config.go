package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/alvor-technologies/iag-platform-go/corsenv"
	"github.com/joho/godotenv"
)

type Config struct {
	Environment string
	ServiceName string
	Port        string
	LogLevel    string

	DatabaseURL string
	RedisURL    string
	AutoMigrate bool

	AuthMode              string
	JWTIssuer             string
	JWKSURL               string
	Audience              string
	ServiceClientID       string
	ServiceClientSecret   string
	AuthTokenURL          string
	CORSOrigins           []string
	GatewayAPIPrefix      string
	PublicTraceBaseURL    string
	PublicAPIURL          string
	SupplyChainBaseURL    string
	SupplyChainAudience   string

	KafkaBrokers          []string
	KafkaConsumerGroup    string
	KafkaSupplyChainTopic string
	KafkaProductionTopic  string
	KafkaQualityTopic     string
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	env := strings.ToLower(strings.TrimSpace(getenv("ENVIRONMENT", "development")))
	authMode := strings.ToLower(strings.TrimSpace(getenv("AUTH_MODE", "jwt")))
	switch authMode {
	case "jwt":
	default:
		return nil, fmt.Errorf("AUTH_MODE must be jwt (got %q)", authMode)
	}

	c := &Config{
		Environment:           env,
		ServiceName:           getenv("SERVICE_NAME", "traceability"),
		Port:                  getenv("PORT", "4011"),
		LogLevel:              getenv("LOG_LEVEL", "info"),
		DatabaseURL:           strings.TrimSpace(os.Getenv("DATABASE_URL")),
		RedisURL:              getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
		AutoMigrate:           getenv("AUTO_MIGRATE", "true") != "false",
		AuthMode:              authMode,
		JWTIssuer:             getenv("JWT_ISSUER", "http://localhost:3001"),
		JWKSURL:               getenv("JWKS_URL", "http://localhost:3001/.well-known/jwks.json"),
		Audience:              getenv("AUDIENCE", "iag.traceability"),
		ServiceClientID:       getenv("SERVICE_CLIENT_ID", "iag-traceability"),
		ServiceClientSecret:   os.Getenv("SERVICE_CLIENT_SECRET"),
		AuthTokenURL:          strings.TrimSpace(getenv("AUTH_TOKEN_URL", "")),
		CORSOrigins:           splitCSV(corsenv.Allowlist("http://localhost:3000,http://localhost:8080")),
		GatewayAPIPrefix:      getenv("GATEWAY_API_PREFIX", "/api/v1/traceability"),
		PublicTraceBaseURL:    getenv("PUBLIC_TRACE_BASE_URL", "http://localhost:8080/api/v1/traceability/public/q"),
		PublicAPIURL:          getenv("PUBLIC_API_URL", "http://localhost:8080"),
		SupplyChainBaseURL:    strings.TrimRight(getenv("SUPPLY_CHAIN_BASE_URL", "http://127.0.0.1:4007"), "/"),
		SupplyChainAudience:   getenv("SUPPLY_CHAIN_AUDIENCE", "iag.supply-chain"),
		KafkaBrokers:          splitCSV(getenv("KAFKA_BROKERS", "")),
		KafkaConsumerGroup:    getenv("KAFKA_CONSUMER_GROUP", "iag.traceability"),
		KafkaSupplyChainTopic: getenv("KAFKA_SUPPLY_CHAIN_TOPIC", "iag.supply-chain"),
		KafkaProductionTopic:  getenv("KAFKA_PRODUCTION_TOPIC", "iag.production"),
		KafkaQualityTopic:     getenv("KAFKA_QUALITY_TOPIC", "iag.quality"),
	}

	if c.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if c.AuthTokenURL == "" {
		c.AuthTokenURL = strings.TrimRight(c.JWTIssuer, "/") + "/oauth/token"
	}
	return c, nil
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
