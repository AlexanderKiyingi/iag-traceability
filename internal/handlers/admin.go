package handlers

import (
	"net/http"
	"strconv"

	"github.com/alvor-technologies/iag-platform-go/apierr"
	"github.com/gin-gonic/gin"
)

func (a *API) ListAPIAuditLogs(c *gin.Context) {
	if a.Audit == nil {
		apierr.Write(c, http.StatusServiceUnavailable, apierr.CodeServiceUnavailable, "audit log not configured")
		return
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
	items, total, err := a.Audit.ListAPIAuditLogs(c.Request.Context(), limit)
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "could not list audit logs")
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total})
}
