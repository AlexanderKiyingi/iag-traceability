package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/alvor-technologies/iag-platform-go/apierr"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iag-traceability/backend/internal/auditlog"
	"iag-traceability/backend/internal/cache"
	"iag-traceability/backend/internal/config"
	"iag-traceability/backend/internal/kafkabus"
	"iag-traceability/backend/internal/metrics"
	"iag-traceability/backend/internal/middleware"
	"iag-traceability/backend/internal/store"
	"iag-traceability/backend/internal/story"
)

type API struct {
	Cfg             *config.Config
	Store           *store.Store
	Audit           *auditlog.Store
	KafkaPub        *kafkabus.Publisher
	QRCache         *cache.JSONCache
	ConsumerMetrics *metrics.Counters
}

func (a *API) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": a.Cfg.ServiceName,
	})
}

func (a *API) Ready(c *gin.Context) {
	ctx := c.Request.Context()
	if err := a.Store.Ping(ctx); err != nil {
		apierr.Write(c, http.StatusServiceUnavailable, apierr.CodeServiceUnavailable, "database unavailable")
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

func (a *API) GetBatchChain(c *gin.Context) {
	businessID := c.Param("businessId")
	events, err := a.Store.ListEventsForEntity(c.Request.Context(), "batch", businessID, 200)
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to load chain")
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"batch_business_id": businessID,
		"chain":             story.BuildChainFromEvents(events),
	})
}

func (a *API) RecordEvent(c *gin.Context) {
	var body struct {
		OccurredAt       *time.Time     `json:"occurred_at"`
		EventType        string         `json:"event_type"`
		EntityType       string         `json:"entity_type"`
		EntityBusinessID string         `json:"entity_business_id"`
		EntityID         *uuid.UUID     `json:"entity_id"`
		RelatedIDs       map[string]any `json:"related_ids"`
		Payload          map[string]any `json:"payload"`
		IdempotencyKey   *string        `json:"idempotency_key"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.BadRequest(c, "invalid JSON body")
		return
	}
	if body.EventType == "" || body.EntityType == "" || body.EntityBusinessID == "" {
		apierr.BadRequest(c, "event_type, entity_type, and entity_business_id are required")
		return
	}
	if err := story.ValidateEventType(body.EventType); err != nil {
		apierr.BadRequest(c, err.Error())
		return
	}
	if body.EventType == "CORRECTION" {
		if claims, ok := middleware.PlatformClaims(c); !ok || !claims.IsSuperuser {
			apierr.Write(c, http.StatusForbidden, apierr.CodeForbidden, "CORRECTION events require superuser")
			return
		}
	}
	if body.RelatedIDs == nil {
		body.RelatedIDs = map[string]any{}
	}
	if body.Payload != nil {
		if nested, ok := body.Payload["related_ids"].(map[string]any); ok {
			for k, v := range nested {
				body.RelatedIDs[k] = v
			}
		}
	}
	occurred := time.Now().UTC()
	if body.OccurredAt != nil {
		occurred = body.OccurredAt.UTC()
	}
	var actorID *uuid.UUID
	if uid, ok := middleware.UserID(c); ok {
		actorID = &uid
	}
	ev, err := a.Store.AppendEvent(c.Request.Context(), store.AppendEventInput{
		OccurredAt:       occurred,
		EventType:        body.EventType,
		EntityType:       body.EntityType,
		EntityBusinessID: body.EntityBusinessID,
		EntityID:         body.EntityID,
		RelatedIDs:       body.RelatedIDs,
		ActorUserID:      actorID,
		SourceService:    "iag-traceability",
		Payload:          body.Payload,
		IdempotencyKey:   body.IdempotencyKey,
	})
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to append event")
		return
	}
	// Refresh the cached lot story so a manually-recorded event (e.g. a CoA or
	// lab result correction) is reflected without waiting for the next live
	// public fetch — parity with the Kafka consumer path.
	a.maybeRebuildLotStory(c.Request.Context(), body.EventType, body.EntityType, body.EntityBusinessID, body.RelatedIDs)
	c.JSON(http.StatusCreated, ev)
}

// maybeRebuildLotStory recomposes and persists the affected lot's story
// projection (and emits scm.lot.story_updated) when a recorded event warrants
// it. Best-effort: failures are swallowed so they never fail the write.
func (a *API) maybeRebuildLotStory(ctx context.Context, eventType, entityType, entityBusinessID string, related map[string]any) {
	if !story.AffectsLotStory(eventType) {
		return
	}
	lotID := ""
	if entityType == "lot" {
		lotID = entityBusinessID
	} else if v, ok := related["lot_business_id"].(string); ok {
		lotID = v
	}
	if lotID == "" {
		return
	}
	if ok, err := story.RebuildStoredLot(ctx, a.Store, lotID, ""); err == nil && ok && a.KafkaPub != nil {
		_ = a.KafkaPub.EmitLotStoryUpdated(ctx, lotID)
	}
}

func (a *API) ListEvents(c *gin.Context) {
	entityType := c.Query("entity_type")
	businessID := c.Query("entity_business_id")
	if entityType == "" || businessID == "" {
		apierr.BadRequest(c, "entity_type and entity_business_id query params are required")
		return
	}
	events, err := a.Store.ListEventsForEntity(c.Request.Context(), entityType, businessID, 200)
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to list events")
		return
	}
	c.JSON(http.StatusOK, gin.H{"events": events})
}

func (a *API) PublishLotQR(c *gin.Context) {
	lotID := c.Param("businessId")
	if err := story.ValidateLotPublish(c.Request.Context(), a.Store, lotID); err != nil {
		if err == story.ErrComplianceGateUnavailable {
			apierr.Write(c, http.StatusServiceUnavailable, apierr.CodeServiceUnavailable, err.Error())
			return
		}
		apierr.Write(c, http.StatusUnprocessableEntity, "COMPLIANCE_FAILED", err.Error())
		return
	}
	token, publicURL, err := story.PublishQR(c.Request.Context(), a.Store, lotID, a.Cfg.PublicTraceBaseURL)
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to publish QR")
		return
	}
	if a.KafkaPub != nil {
		_ = a.KafkaPub.EmitLotQRPublished(c.Request.Context(), lotID, token, publicURL)
	}
	if a.QRCache != nil {
		a.QRCache.Delete(c.Request.Context(), publicQRCacheKey(token))
	}
	c.JSON(http.StatusOK, gin.H{
		"lot_business_id": lotID,
		"public_token":    token,
		"public_url":      publicURL,
	})
}

func (a *API) RevokeLotQR(c *gin.Context) {
	lotID := c.Param("businessId")
	qr, tokenErr := a.Store.GetLotQRByLotID(c.Request.Context(), lotID)
	if err := a.Store.RevokeLotQR(c.Request.Context(), lotID); err != nil {
		if err == store.ErrNotFound {
			apierr.NotFound(c, "lot QR not found or already revoked")
			return
		}
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to revoke QR")
		return
	}
	if tokenErr == nil && a.QRCache != nil {
		a.QRCache.Delete(c.Request.Context(), publicQRCacheKey(qr.PublicToken))
	}
	if a.KafkaPub != nil {
		token := ""
		if tokenErr == nil {
			token = qr.PublicToken
		}
		_ = a.KafkaPub.EmitLotQRRevoked(c.Request.Context(), lotID, token)
	}
	c.JSON(http.StatusOK, gin.H{"lot_business_id": lotID, "revoked": true})
}

func (a *API) PublicQR(c *gin.Context) {
	token := c.Param("token")
	var payload *story.PublicPayload
	cacheKey := publicQRCacheKey(token)
	if a.QRCache != nil {
		err := a.QRCache.GetOrSet(c.Request.Context(), cacheKey, &payload, func() (any, error) {
			return story.ResolvePublicQR(c.Request.Context(), a.Store, token, a.Cfg.PublicTraceBaseURL)
		})
		if err != nil {
			if err == store.ErrNotFound {
				apierr.NotFound(c, "QR not found or revoked")
				return
			}
			apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to resolve QR")
			return
		}
	} else {
		var err error
		payload, err = story.ResolvePublicQR(c.Request.Context(), a.Store, token, a.Cfg.PublicTraceBaseURL)
		if err != nil {
			if err == store.ErrNotFound {
				apierr.NotFound(c, "QR not found or revoked")
				return
			}
			apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to resolve QR")
			return
		}
	}
	c.Header("Cache-Control", "public, max-age=300, s-maxage=3600")
	c.Header("Vary", "Accept-Encoding")
	c.JSON(http.StatusOK, payload)
}

func publicQRCacheKey(token string) string {
	return "trace:public:q:" + token
}

func (a *API) PublicQRPng(c *gin.Context) {
	token := c.Param("token")
	payload, err := story.ResolvePublicQR(c.Request.Context(), a.Store, token, a.Cfg.PublicTraceBaseURL)
	if err != nil {
		if err == store.ErrNotFound {
			apierr.NotFound(c, "QR not found or revoked")
			return
		}
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to resolve QR")
		return
	}
	png, err := qrPNG(payload.PublicURL)
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to generate QR image")
		return
	}
	c.Data(http.StatusOK, "image/png", png)
}
