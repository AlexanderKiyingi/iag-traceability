package middleware

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/alvor-technologies/iag-platform-go/apierr"
	"github.com/alvor-technologies/iag-platform-go/authclient"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iag-traceability/backend/internal/ctxkeys"
)

type PlatformAuth struct {
	authMode string
	verifier *authclient.Verifier
}

type PlatformAuthOptions struct {
	Mode     string
	Verifier *authclient.Verifier
}

func NewPlatformAuth(opts PlatformAuthOptions) *PlatformAuth {
	return &PlatformAuth{
		authMode: opts.Mode,
		verifier: opts.Verifier,
	}
}

func SetAuthMode(c *gin.Context, mode string) {
	c.Set(ctxkeys.AuthMode, mode)
}

func AuthMode(c *gin.Context) string {
	v, _ := c.Get(ctxkeys.AuthMode)
	s, _ := v.(string)
	return s
}

func isPublicProbePath(path string) bool {
	switch path {
	case "/health", "/healthz", "/ready":
		return true
	default:
		return false
	}
}

func isPublicTracePath(path string) bool {
	return strings.HasPrefix(path, "/public/q/")
}

func (m *PlatformAuth) AttachPrincipal() gin.HandlerFunc {
	return func(c *gin.Context) {
		SetAuthMode(c, m.authMode)
		if isPublicProbePath(c.Request.URL.Path) || isPublicTracePath(c.Request.URL.Path) {
			c.Next()
			return
		}
		if m.authMode == "jwt" {
			m.fromJWT(c)
			return
		}
		c.Next()
	}
}

func (m *PlatformAuth) RequireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		if isPublicProbePath(c.Request.URL.Path) || isPublicTracePath(c.Request.URL.Path) {
			c.Next()
			return
		}
		if _, ok := UserID(c); !ok {
			apierr.Unauthorized(c, "authentication required")
			return
		}
		c.Next()
	}
}

func (m *PlatformAuth) fromJWT(c *gin.Context) {
	if m.verifier == nil {
		apierr.Write(c, http.StatusServiceUnavailable, apierr.CodeServiceUnavailable, "JWT verifier not configured")
		return
	}
	tokenStr := bearerToken(c)
	if tokenStr == "" {
		c.Next()
		return
	}
	claims, err := m.verifier.Verify(tokenStr)
	if err != nil {
		apierr.Unauthorized(c, "invalid or expired token")
		return
	}
	userID, _ := uuid.Parse(claims.Subject)
	setPrincipal(c, userID, claims, claims.Permissions)
	c.Next()
}

func setPrincipal(c *gin.Context, userID uuid.UUID, claims *authclient.Claims, perms []string) {
	c.Set(ctxkeys.UserID, userID)
	c.Set(ctxkeys.Claims, claims)
	c.Set(ctxkeys.Permissions, perms)
}

func UserID(c *gin.Context) (uuid.UUID, bool) {
	v, ok := c.Get(ctxkeys.UserID)
	if !ok {
		return uuid.Nil, false
	}
	id, ok := v.(uuid.UUID)
	return id, ok
}

func PlatformClaims(c *gin.Context) (*authclient.Claims, bool) {
	v, ok := c.Get(ctxkeys.Claims)
	if !ok {
		return nil, false
	}
	cl, ok := v.(*authclient.Claims)
	return cl, ok
}

func RequirePermission(code string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if claims, ok := PlatformClaims(c); ok && claims.HasPermission(code) {
			c.Next()
			return
		}
		apierr.WriteWith(c, http.StatusForbidden, apierr.CodeForbidden,
			"permission denied: "+code, gin.H{"required_permission": code})
	}
}

func bearerToken(c *gin.Context) string {
	header := c.GetHeader("Authorization")
	if strings.HasPrefix(header, "Bearer ") {
		return strings.TrimPrefix(header, "Bearer ")
	}
	return ""
}

// VerifyBearerToken validates a raw JWT (reserved for future WebSocket use).
func (m *PlatformAuth) VerifyBearerToken(tokenStr string) (uuid.UUID, *authclient.Claims, error) {
	if m.verifier == nil {
		return uuid.Nil, nil, fmt.Errorf("jwt verifier not configured")
	}
	claims, err := m.verifier.Verify(tokenStr)
	if err != nil {
		return uuid.Nil, nil, err
	}
	userID, _ := uuid.Parse(claims.Subject)
	return userID, claims, nil
}
