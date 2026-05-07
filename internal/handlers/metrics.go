package handlers

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterMetrics wires /metrics onto the router. Per golden-standard this
// endpoint is scoped to the internal scrape client via NetworkPolicy —
// don't add auth middleware here, the NetworkPolicy does the access control.
func RegisterMetrics(r gin.IRouter) {
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
}
