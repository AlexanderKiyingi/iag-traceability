package handlers

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/alvor-technologies/iag-platform-go/apierr"
	"github.com/gin-gonic/gin"

	"iag-traceability/backend/internal/anchor"
	"iag-traceability/backend/internal/store"
)

// ListAnchors returns notarization anchors newest-first (L2/L3 audit ledger).
func (a *API) ListAnchors(c *gin.Context) {
	if a.Anchor == nil {
		anchorDisabled(c)
		return
	}
	rows, err := a.Anchor.List(c.Request.Context(), parseLimit(c.Query("limit")))
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to list anchors")
		return
	}
	c.JSON(http.StatusOK, gin.H{"anchors": rows, "count": len(rows)})
}

// LatestAnchor returns the most recent notarization, plus the verifier material
// (algo + public key) an auditor needs to check the signature offline.
func (a *API) LatestAnchor(c *gin.Context) {
	if a.Anchor == nil {
		anchorDisabled(c)
		return
	}
	row, err := a.Anchor.Latest(c.Request.Context())
	if errors.Is(err, store.ErrNotFound) {
		c.JSON(http.StatusOK, gin.H{"anchor": nil})
		return
	}
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to load latest anchor")
		return
	}
	algo, keyID, pub := a.Anchor.SignerInfo()
	c.JSON(http.StatusOK, gin.H{
		"anchor": row,
		"signer": gin.H{"algo": algo, "keyId": keyID, "publicKey": pub},
	})
}

// CreateAnchor notarizes all custody blocks since the last anchor. Staff-only.
func (a *API) CreateAnchor(c *gin.Context) {
	if a.Anchor == nil {
		anchorDisabled(c)
		return
	}
	row, err := a.Anchor.CreateAnchor(c.Request.Context())
	if errors.Is(err, anchor.ErrNothingToAnchor) {
		c.JSON(http.StatusOK, gin.H{"created": false, "reason": "no new custody events since last anchor"})
		return
	}
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to create anchor")
		return
	}
	c.JSON(http.StatusCreated, gin.H{"created": true, "anchor": row})
}

// AnchorProof returns a Merkle inclusion proof for one custody block (?seq=N)
// against the signed root that notarized it.
func (a *API) AnchorProof(c *gin.Context) {
	if a.Anchor == nil {
		anchorDisabled(c)
		return
	}
	seq, err := strconv.ParseInt(c.Query("seq"), 10, 64)
	if err != nil || seq <= 0 {
		apierr.Write(c, http.StatusBadRequest, apierr.CodeValidation, "seq query param (positive integer) is required")
		return
	}
	res, err := a.Anchor.Proof(c.Request.Context(), seq)
	if errors.Is(err, store.ErrNotFound) {
		apierr.Write(c, http.StatusNotFound, apierr.CodeNotFound, "custody block not found or not yet anchored")
		return
	}
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to build proof")
		return
	}
	c.JSON(http.StatusOK, res)
}

func anchorDisabled(c *gin.Context) {
	apierr.Write(c, http.StatusServiceUnavailable, apierr.CodeServiceUnavailable, "notarization (anchoring) is not enabled on this deployment")
}
