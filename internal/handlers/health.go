// Package handlers implements HTTP handlers for the service.
package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HealthHandler serves /health/live and /health/ready.
// Unauthenticated per golden-standard contract #1.
type HealthHandler struct {
	pool    *pgxpool.Pool
	version string
}

// NewHealthHandler constructs a HealthHandler.
func NewHealthHandler(pool *pgxpool.Pool, version string) *HealthHandler {
	return &HealthHandler{pool: pool, version: version}
}

// RegisterRoutes wires the health routes onto the given router. GET and
// HEAD are both registered: gin.New() doesn't auto-dispatch HEAD to a
// GET handler, and external uptime checks / load balancers routinely
// use HEAD on /health/* to avoid transferring the body.
func (h *HealthHandler) RegisterRoutes(r gin.IRouter) {
	r.GET("/health/live", h.live)
	r.HEAD("/health/live", h.live)
	r.GET("/health/ready", h.ready)
	r.HEAD("/health/ready", h.ready)
}

// live responds 200 once the process is running.
//
// @Summary Liveness probe
// @Tags health
// @Success 200	{object}	map[string]string
// @Router /health/live [get]
func (h *HealthHandler) live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "live",
		"version": h.version,
	})
}

// ready responds 200 only when dependencies (DB) are reachable.
//
// In shell mode (no DB pool), returns 200 with mode=shell — this allows the
// template's own self-deploy to pass readiness without a real DB.
//
// @Summary Readiness probe
// @Tags health
// @Success 200	{object}	map[string]string
// @Failure 503	{object}	map[string]string
// @Router /health/ready [get]
func (h *HealthHandler) ready(c *gin.Context) {
	if h.pool == nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  "ready",
			"version": h.version,
			"mode":    "shell",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	if err := h.pool.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not-ready",
			"error":  err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "ready",
		"version": h.version,
	})
}
