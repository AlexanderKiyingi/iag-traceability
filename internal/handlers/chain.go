package handlers

import (
	"net/http"

	"github.com/alvor-technologies/iag-platform-go/apierr"
	"github.com/gin-gonic/gin"
)

// GetCustodyChain returns the tamper-evident custody hash-chain in sequence order.
// Optional ?batchId= scopes it to one batch; ?limit= caps the result. When
// CHAIN_ENABLED is off (or nothing chained yet) the chain is simply empty.
func (a *API) GetCustodyChain(c *gin.Context) {
	batchID := c.Query("batchId")
	limit := parseLimit(c.Query("limit"))
	blocks, err := a.Store.ListChain(c.Request.Context(), batchID, limit)
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to load custody chain")
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"chain":   blocks,
		"count":   len(blocks),
		"enabled": a.Store.ChainEnabled(),
	})
}

// VerifyCustodyChain walks the whole chain and reports validity + any anomalies.
func (a *API) VerifyCustodyChain(c *gin.Context) {
	res, err := a.Store.VerifyChain(c.Request.Context())
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "failed to verify custody chain")
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"isValid":      res.IsValid,
		"anomalies":    res.Anomalies,
		"checkedCount": res.CheckedCount,
		"enabled":      a.Store.ChainEnabled(),
	})
}
