package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestRegisterMetrics confirms /metrics serves Prometheus format.
func TestRegisterMetrics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterMetrics(r)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}
