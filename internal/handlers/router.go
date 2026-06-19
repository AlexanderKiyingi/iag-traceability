package handlers

import (
	"net/http"
	"strings"

	"github.com/alvor-technologies/iag-platform-go/middleware"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"iag-traceability/backend/internal/auditlog"
	appmw "iag-traceability/backend/internal/middleware"
)

type RouterDeps struct {
	API               *API
	Audit             *auditlog.Store
	PlatformAuth      *appmw.PlatformAuth
	CORSOrigins       []string
	PublicRatePerMin  float64
	PublicRateBurst   int
}

func NewRouter(deps RouterDeps) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	// otelgin first so the server span wraps the whole request.
	r.Use(otelgin.Middleware("iag-traceability"))
	r.Use(gin.Recovery())
	r.Use(middleware.RequestID())
	r.Use(securityHeaders())
	r.Use(corsMiddleware(deps.CORSOrigins))

	api := deps.API
	if deps.PlatformAuth != nil {
		r.Use(deps.PlatformAuth.AttachPrincipal())
	}
	r.Use(appmw.RequestAudit(deps.Audit))

	r.GET("/health", api.Health)
	r.GET("/healthz", api.Health)
	r.GET("/ready", api.Ready)

	public := r.Group("/public")
	public.Use(appmw.PublicRateLimit(deps.PublicRatePerMin, deps.PublicRateBurst))
	{
		public.GET("/q/:token", api.PublicQR)
		public.GET("/q/:token/qr.png", api.PublicQRPng)
	}

	v1 := r.Group("/api/v1")
	if deps.PlatformAuth != nil {
		v1.Use(deps.PlatformAuth.RequireAuth())
	}
	{
		v1.GET("/batches/:businessId/chain", appmw.RequirePermission("traceability.view_chain"), api.GetBatchChain)
		v1.GET("/events", appmw.RequirePermission("traceability.view_events"), api.ListEvents)
		v1.POST("/events", appmw.RequirePermission("traceability.add_trace_event"), api.RecordEvent)
		v1.POST("/lots/:businessId/publish", appmw.RequirePermission("traceability.publish_qr"), api.PublishLotQR)
		v1.POST("/lots/:businessId/revoke", appmw.RequirePermission("traceability.publish_qr"), api.RevokeLotQR)

		admin := v1.Group("/admin")
		admin.Use(appmw.RequirePermission("audit.view_api_log"))
		{
			admin.GET("/audit-logs", api.ListAPIAuditLogs)
			admin.GET("/monitoring/summary", api.AdminMonitoringSummary)
			admin.GET("/monitoring/activity", api.AdminMonitoringActivity)
			admin.GET("/monitoring/dead-letters", api.AdminDeadLetters)
		}
	}

	return r
}

func securityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Next()
	}
}

func corsMiddleware(allowed []string) gin.HandlerFunc {
	allowSet := map[string]bool{}
	for _, o := range allowed {
		if t := strings.TrimSpace(o); t != "" {
			allowSet[t] = true
		}
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" && allowSet[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
