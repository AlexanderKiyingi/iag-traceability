package handlers

import (
	"errors"
	"net/http"
	"unicode/utf8"

	"github.com/alvor-technologies/iag-platform-go/apierr"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iag-traceability/backend/internal/middleware"
	"iag-traceability/backend/internal/store"
)

const (
	maxStoryTitleChars = 200
	maxStoryBodyChars  = 5000
	maxStoryBodyBytes  = 64 * 1024
)

// publicURLFor builds a QR's public address exactly as PublishLotQR does.
func (a *API) publicURLFor(token string) string {
	base := ""
	if a.Cfg != nil {
		base = a.Cfg.PublicTraceBaseURL
	}
	return base + "/" + token
}

// ListQRCodes is the QR register: every lot QR (live and revoked) with its
// story title, override and farmer names.
func (a *API) ListQRCodes(c *gin.Context) {
	limit := parseLimit(c.Query("limit"))
	rows, err := a.Store.ListQRRegister(c.Request.Context(), store.QRRegisterFilter{
		Status: c.Query("status"),
		Q:      c.Query("q"),
		Limit:  limit + 1,
	})
	if err != nil {
		if errors.Is(err, store.ErrInvalidFilter) {
			apierr.BadRequest(c, "status must be one of: live, revoked")
			return
		}
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to list QR codes")
		return
	}
	hasMore := false
	if len(rows) > limit {
		rows, hasMore = rows[:limit], true
	}
	for i := range rows {
		rows[i].PublicURL = a.publicURLFor(rows[i].Token)
	}
	c.JSON(http.StatusOK, gin.H{"qr_codes": rows, "has_more": hasMore, "limit": limit})
}

// GetQRCode returns one lot's register row (live or revoked).
func (a *API) GetQRCode(c *gin.Context) {
	row, ok := a.loadQRRow(c, c.Param("lotBusinessId"))
	if !ok {
		return
	}
	c.JSON(http.StatusOK, row)
}

func (a *API) loadQRRow(c *gin.Context, lotID string) (store.QRRegisterRow, bool) {
	row, err := a.Store.GetQRRegisterRow(c.Request.Context(), lotID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			apierr.NotFound(c, "lot QR not found")
			return row, false
		}
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to load QR code")
		return row, false
	}
	row.PublicURL = a.publicURLFor(row.Token)
	return row, true
}

// PatchQRStory edits the public story of a published lot without
// re-publishing it. Re-publishing rotates the token and breaks every printed
// label, so this path never touches lot_qr_codes.
//
// story_title / story_body: omitted = unchanged, "" = clear that override.
func (a *API) PatchQRStory(c *gin.Context) {
	lotID := c.Param("lotBusinessId")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxStoryBodyBytes)
	var body struct {
		StoryTitle *string `json:"story_title"`
		StoryBody  *string `json:"story_body"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.BadRequest(c, "invalid or oversized JSON body")
		return
	}
	if reason := validateStoryPatch(body.StoryTitle, body.StoryBody); reason != "" {
		apierr.BadRequest(c, reason)
		return
	}
	var actorID *uuid.UUID
	if uid, ok := middleware.UserID(c); ok {
		actorID = &uid
	}
	err := a.Store.SetStoryOverride(c.Request.Context(), lotID, store.StoryOverridePatch{
		StoryTitle:      body.StoryTitle,
		StoryBody:       body.StoryBody,
		UpdatedBy:       middleware.ActorName(c),
		UpdatedByUserID: actorID,
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			apierr.NotFound(c, "lot QR not found")
			return
		}
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to update story")
		return
	}
	row, ok := a.loadQRRow(c, lotID)
	if !ok {
		return
	}
	// The public payload is cached per token; drop it so the next scan serves
	// the edited story rather than the cached one.
	if a.QRCache != nil {
		a.QRCache.Delete(c.Request.Context(), publicQRCacheKey(row.Token))
	}
	c.JSON(http.StatusOK, row)
}

func validateStoryPatch(title, body *string) string {
	if title == nil && body == nil {
		return "story_title or story_body is required"
	}
	if title != nil && utf8.RuneCountInString(*title) > maxStoryTitleChars {
		return "story_title must be at most 200 characters"
	}
	if body != nil && utf8.RuneCountInString(*body) > maxStoryBodyChars {
		return "story_body must be at most 5000 characters"
	}
	return ""
}
