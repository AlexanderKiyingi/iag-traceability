package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

type limiterMap struct {
	mu       sync.Mutex
	buckets  map[string]*rate.Limiter
	rate     rate.Limit
	burst    int
	lastSeen map[string]time.Time
}

func newLimiterMap(perMinute float64, burst int) *limiterMap {
	if perMinute <= 0 {
		perMinute = 60
	}
	if burst <= 0 {
		burst = 10
	}
	return &limiterMap{
		buckets:  map[string]*rate.Limiter{},
		lastSeen: map[string]time.Time{},
		rate:     rate.Limit(perMinute / 60.0),
		burst:    burst,
	}
}

func (lm *limiterMap) get(key string) *rate.Limiter {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	l, ok := lm.buckets[key]
	if !ok {
		l = rate.NewLimiter(lm.rate, lm.burst)
		lm.buckets[key] = l
	}
	lm.lastSeen[key] = time.Now()
	if len(lm.buckets) > 1024 {
		cutoff := time.Now().Add(-time.Hour)
		for k, t := range lm.lastSeen {
			if t.Before(cutoff) {
				delete(lm.buckets, k)
				delete(lm.lastSeen, k)
			}
		}
	}
	return l
}

// PublicRateLimit limits anonymous public QR traffic per client IP.
func PublicRateLimit(perMinute float64, burst int) gin.HandlerFunc {
	lm := newLimiterMap(perMinute, burst)
	return func(c *gin.Context) {
		key := c.ClientIP()
		if key == "" {
			// Fail closed, not open: an unidentifiable client shares a single
			// fallback bucket rather than bypassing the limiter entirely.
			key = "_unknown"
		}
		if !lm.get(key).Allow() {
			c.Header("Retry-After", "60")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": gin.H{"code": "RATE_LIMIT", "message": "rate limit exceeded"},
			})
			return
		}
		c.Next()
	}
}
