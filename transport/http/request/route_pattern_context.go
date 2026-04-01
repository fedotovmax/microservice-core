package request

import (
	"context"

	"github.com/go-chi/chi/v5"
)

func RoutePattern(ctx context.Context) string {
	return chi.RouteContext(ctx).RoutePattern()
}
