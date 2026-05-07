package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestHealthHandler_Live confirms /health/live responds 200 regardless of DB.
func TestHealthHandler_Live(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewHealthHandler(nil, "test-version")
	r := gin.New()
	h.RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if body["status"] != "live" {
		t.Errorf("status=%q want=live", body["status"])
	}
	if body["version"] != "test-version" {
		t.Errorf("version=%q want=test-version", body["version"])
	}
}

// TestHealthHandler_ReadyShellMode confirms /health/ready returns 200 when
// pool is nil (shell mode) — the load-bearing behaviour that lets the
// template self-deploy without a real DB.
func TestHealthHandler_ReadyShellMode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewHealthHandler(nil, "test-version")
	r := gin.New()
	h.RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 in shell mode, got %d", w.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if body["mode"] != "shell" {
		t.Errorf("mode=%q want=shell", body["mode"])
	}
}
