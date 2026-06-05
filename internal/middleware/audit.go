package middleware

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"iag-traceability/backend/internal/auditlog"
)

func ActorName(c *gin.Context) string {
	if claims, ok := PlatformClaims(c); ok && claims != nil {
		if n := strings.TrimSpace(claims.Name); n != "" {
			return n
		}
		if e := strings.TrimSpace(claims.Email); e != "" {
			return e
		}
		if claims.Subject != "" {
			return claims.Subject
		}
	}
	return "anonymous"
}

func RequestAudit(store *auditlog.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		if store == nil {
			return
		}
		path := c.Request.URL.Path
		if isPublicProbePath(path) || isPublicTracePath(path) {
			return
		}
		duration := int(time.Since(start).Milliseconds())
		_ = store.LogAPIRequest(
			c.Request.Context(),
			c.Request.Method,
			path,
			c.Writer.Status(),
			ActorName(c),
			duration,
			c.ClientIP(),
		)
	}
}
