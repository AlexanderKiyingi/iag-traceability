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

func (a *API) AdminMonitoringSummary(c *gin.Context) {
	if a.Audit == nil {
		apierr.Write(c, http.StatusServiceUnavailable, apierr.CodeServiceUnavailable, "audit log not configured")
		return
	}
	kafkaEnabled := len(a.Cfg.KafkaBrokers) > 0
	summary, err := a.Audit.MonitoringSummary(c.Request.Context(), kafkaEnabled)
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "monitoring failed")
		return
	}
	c.JSON(http.StatusOK, summary)
}

func (a *API) AdminMonitoringActivity(c *gin.Context) {
	if a.Audit == nil {
		apierr.Write(c, http.StatusServiceUnavailable, apierr.CodeServiceUnavailable, "audit log not configured")
		return
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "30"))
	items, err := a.Audit.APIMonitoringActivity(c.Request.Context(), limit)
	if err != nil {
		apierr.Write(c, http.StatusInternalServerError, apierr.CodeInternal, "activity failed")
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}
