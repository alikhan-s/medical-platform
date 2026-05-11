package cache

import (
	"context"
	"errors"
)

var ErrCacheMiss = errors.New("cache miss")

type CacheRepository interface {
	Get(ctx context.Context, entity, id string, dest any) error
	Set(ctx context.Context, entity, id string, value any) error
	Delete(ctx context.Context, entity, id string) error
}
