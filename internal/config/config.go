package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

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

	AuthMode            string
	JWTIssuer           string
	JWKSURL             string
	Audience            string
	ServiceClientID     string
	ServiceClientSecret string
	AuthTokenURL        string
	// AuthServiceURL is the auth HTTP root for outbound calls (permissions register).
	// In Docker this differs from JWT_ISSUER when tokens use iss=localhost.
	AuthServiceURL      string
	CORSOrigins         []string
	GatewayAPIPrefix    string
	PublicTraceBaseURL  string
	PublicAPIURL        string
	SupplyChainBaseURL  string
	SupplyChainAudience string

	KafkaBrokers          []string
	KafkaClientID         string
	KafkaConsumerGroup    string
	KafkaSupplyChainTopic string
	KafkaProductionTopic  string
	KafkaQualityTopic     string

	PublicRateLimitPerMin   float64
	PublicRateBurst         int
	CacheTTL                time.Duration
	StoryPlaceholderJourney bool

	// TrustedProxies is the CIDR/IP allowlist Gin uses to honor X-Forwarded-For
	// when deriving the client IP for rate limiting. Empty (the default) means
	// trust NO proxy headers — ClientIP is the direct peer, so the per-IP rate
	// limiter cannot be defeated by a forged X-Forwarded-For. Set this to the
	// gateway's address(es) in production to restore true per-end-user limiting.
	TrustedProxies []string
	// PublicPreciseGeo controls whether the unauthenticated public QR payload
	// exposes precise farm/farmer GPS coordinates. Default false: coordinates are
	// coarsened (~1km) to protect smallholder location privacy.
	PublicPreciseGeo bool

	// ChainEnabled turns on the tamper-evident custody hash-chain: batch trace
	// events are appended to custody_chain (hash-linked, advisory-locked) and the
	// /chain/verify endpoint walks it. Default false — the chain is opt-in so it
	// does not affect event ingestion until a deployment enables it.
	ChainEnabled bool

	// --- L2 notarization / L3 public anchoring (all opt-in) ---
	// AnchorEnabled turns on the /v1/anchors API and the background notarizer.
	AnchorEnabled bool
	// AnchorInterval is how often the background job auto-notarizes new blocks.
	// Zero disables the job (anchors can still be created via POST /v1/anchors).
	AnchorInterval time.Duration
	// AnchorSigningAlgo: none | hmac-sha256 | ed25519.
	AnchorSigningAlgo string
	// AnchorKeyID identifies the signing key (stored on each anchor for rotation).
	AnchorKeyID string
	// AnchorSigningKey is the hex key material (HMAC secret or ed25519 seed).
	AnchorSigningKey string
	// AnchorProvider selects the L3 public ledger: none | polygon. Default none.
	AnchorProvider string
	// AnchorRPCURL / AnchorContract configure the L3 provider when not "none".
	AnchorRPCURL   string
	AnchorContract string

	// Vault Transit signing (used when AnchorSigningAlgo=vault-transit): the L2
	// signing key stays in Vault and never touches the service process.
	VaultAddr         string
	VaultToken        string
	VaultTransitMount string
	VaultTransitKey   string
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
		Environment:             env,
		ServiceName:             getenv("SERVICE_NAME", "traceability"),
		Port:                    getenv("PORT", "4011"),
		LogLevel:                getenv("LOG_LEVEL", "info"),
		DatabaseURL:             strings.TrimSpace(os.Getenv("DATABASE_URL")),
		RedisURL:                getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
		AutoMigrate:             getenv("AUTO_MIGRATE", "true") != "false",
		ChainEnabled:            getenv("CHAIN_ENABLED", "false") == "true",
		AnchorEnabled:           getEnvBool("ANCHOR_ENABLED", false),
		AnchorInterval:          getEnvDuration("ANCHOR_INTERVAL", "0s"),
		AnchorSigningAlgo:       getenv("ANCHOR_SIGNING_ALGO", "none"),
		AnchorKeyID:             getenv("ANCHOR_KEY_ID", ""),
		AnchorSigningKey:        strings.TrimSpace(os.Getenv("ANCHOR_SIGNING_KEY")),
		AnchorProvider:          getenv("ANCHOR_PROVIDER", "none"),
		AnchorRPCURL:            strings.TrimSpace(os.Getenv("ANCHOR_RPC_URL")),
		AnchorContract:          strings.TrimSpace(os.Getenv("ANCHOR_CONTRACT_ADDRESS")),
		VaultAddr:               strings.TrimSpace(os.Getenv("VAULT_ADDR")),
		VaultToken:              strings.TrimSpace(os.Getenv("VAULT_TOKEN")),
		VaultTransitMount:       getenv("VAULT_TRANSIT_MOUNT", "transit"),
		VaultTransitKey:         strings.TrimSpace(os.Getenv("VAULT_TRANSIT_KEY")),
		AuthMode:                authMode,
		JWTIssuer:               getenv("JWT_ISSUER", "http://localhost:3001"),
		JWKSURL:                 getenv("JWKS_URL", "http://localhost:3001/.well-known/jwks.json"),
		Audience:                getenv("AUDIENCE", "iag.traceability"),
		ServiceClientID:         getenv("SERVICE_CLIENT_ID", "iag-traceability"),
		ServiceClientSecret:     os.Getenv("SERVICE_CLIENT_SECRET"),
		AuthTokenURL:            strings.TrimSpace(getenv("AUTH_TOKEN_URL", "")),
		CORSOrigins:             splitCSV(corsenv.Allowlist("http://localhost:3000,http://localhost:8080")),
		GatewayAPIPrefix:        getenv("GATEWAY_API_PREFIX", "/api/v1/traceability"),
		PublicTraceBaseURL:      getenv("PUBLIC_TRACE_BASE_URL", "http://localhost:8080/api/v1/traceability/public/q"),
		PublicAPIURL:            getenv("PUBLIC_API_URL", "http://localhost:8080"),
		SupplyChainBaseURL:      strings.TrimRight(getenv("SUPPLY_CHAIN_BASE_URL", "http://127.0.0.1:4007"), "/"),
		SupplyChainAudience:     getenv("SUPPLY_CHAIN_AUDIENCE", "iag.supply-chain"),
		KafkaBrokers:            splitCSV(getenv("KAFKA_BROKERS", "")),
		KafkaClientID:           getenv("KAFKA_CLIENT_ID", "iag-traceability"),
		KafkaConsumerGroup:      getenv("KAFKA_CONSUMER_GROUP", "iag.traceability"),
		KafkaSupplyChainTopic:   getenv("KAFKA_SUPPLY_CHAIN_TOPIC", "iag.supply-chain"),
		KafkaProductionTopic:    getenv("KAFKA_PRODUCTION_TOPIC", "iag.production"),
		KafkaQualityTopic:       getenv("KAFKA_QUALITY_TOPIC", "iag.quality"),
		PublicRateLimitPerMin:   getEnvFloat("PUBLIC_RATE_LIMIT", 60),
		PublicRateBurst:         getEnvInt("PUBLIC_RATE_BURST", 10),
		CacheTTL:                getEnvDuration("CACHE_TTL", "5m"),
		StoryPlaceholderJourney: getEnvBool("STORY_PLACEHOLDER_JOURNEY", env != "production"),
		TrustedProxies:          splitCSV(getenv("TRUSTED_PROXIES", strings.Join(PrivateNetworks, ","))),
		PublicPreciseGeo:        getEnvBool("PUBLIC_PRECISE_GEO", false),
	}

	if c.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if c.AuthTokenURL == "" {
		c.AuthTokenURL = strings.TrimRight(c.JWTIssuer, "/") + "/oauth/token"
	}
	c.AuthServiceURL = strings.TrimSpace(os.Getenv("AUTH_SERVICE_URL"))
	if c.AuthServiceURL == "" {
		c.AuthServiceURL = authServiceBaseFromTokenURL(c.AuthTokenURL)
	}
	if c.AuthServiceURL == "" {
		c.AuthServiceURL = strings.TrimRight(c.JWTIssuer, "/")
	}
	return c, nil
}

func authServiceBaseFromTokenURL(tokenURL string) string {
	tokenURL = strings.TrimSpace(tokenURL)
	if tokenURL == "" {
		return ""
	}
	if u, err := url.Parse(tokenURL); err == nil && u.Scheme != "" && u.Host != "" {
		u.Path = ""
		u.RawQuery = ""
		u.Fragment = ""
		return strings.TrimRight(u.String(), "/")
	}
	if i := strings.LastIndex(tokenURL, "/oauth/token"); i > 0 {
		return strings.TrimRight(tokenURL[:i], "/")
	}
	return strings.TrimRight(tokenURL, "/")
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

func getEnvDuration(key, def string) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		v = def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		d, _ = time.ParseDuration(def)
	}
	return d
}

func getEnvFloat(key string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func getEnvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getEnvBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

// PrivateNetworks are the ranges a gateway on the platform's internal network
// connects from. Trusting X-Forwarded-For from these by default lets the
// public rate limiter see each visitor behind the gateway, instead of one
// shared bucket for everyone; a client on the public internet is never in
// them, so it still cannot forge its address.
var PrivateNetworks = []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.0/8", "fc00::/7", "::1/128"}

// PublicTraceURLConfigured reports whether QR links would point somewhere a
// phone can reach. The default is a localhost address, and a QR printed with
// it leads nowhere.
func (c *Config) PublicTraceURLConfigured() bool {
	u := strings.ToLower(strings.TrimSpace(c.PublicTraceBaseURL))
	if u == "" {
		return false
	}
	for _, local := range []string{"localhost", "127.0.0.1", "0.0.0.0", "[::1]"} {
		if strings.Contains(u, local) {
			return false
		}
	}
	return true
}

// LocalEnvironment is development or test, where a localhost QR is fine.
func (c *Config) LocalEnvironment() bool {
	switch c.Environment {
	case "", "development", "dev", "local", "test":
		return true
	}
	return false
}
