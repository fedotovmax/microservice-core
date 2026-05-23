package elasticsearch

import (
	"context"

	"github.com/elastic/go-elasticsearch/v9"
)

type Client interface {
	Native() *elasticsearch.TypedClient
	Stop(ctx context.Context) error
}
