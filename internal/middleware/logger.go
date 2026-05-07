// Package middleware contains gin middleware shared across the service.
package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

// RequestLogger is a minimal structured-JSON request logger via zerolog.
//
// Per golden-standard contract #7: structured JSON logs via slog with trace
// context propagation. We use zerolog (not slog) because go-common is
// zerolog-based; when go-common adds slog support this will migrate.
func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		c.Next()

		log.Info().
			Str("method", method).
			Str("path", path).
			Int("status", c.Writer.Status()).
			Dur("duration", time.Since(start)).
			Str("client_ip", c.ClientIP()).
			Msg("request")
	}
}
