package handlers

import (
	"net/http"
	"strings"

	"github.com/alvor-technologies/iag-platform-go/middleware"
	"github.com/gin-gonic/gin"

	appmw "iag-traceability/backend/internal/middleware"
)

type RouterDeps struct {
	API          *API
	PlatformAuth *appmw.PlatformAuth
}

func NewRouter(deps RouterDeps) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(middleware.RequestID())
	r.Use(securityHeaders())
	r.Use(corsMiddleware())

	api := deps.API
	if deps.PlatformAuth != nil {
		r.Use(deps.PlatformAuth.AttachPrincipal())
	}

	r.GET("/health", api.Health)
	r.GET("/healthz", api.Health)
	r.GET("/ready", api.Ready)

	public := r.Group("/public")
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

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
		}
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

func splitOrigins(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
