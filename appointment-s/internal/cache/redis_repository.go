package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisRepository struct {
	client  *redis.Client
	service string
	ttl     time.Duration
}

func NewRedisRepository(addr, service string) *RedisRepository {
	ttl := ttlFromEnv()
	client := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("[cache] WARN: redis ping failed at %s: %v (continuing with degraded cache)", addr, err)
	} else {
		log.Printf("[cache] connected to redis at %s (service=%s, ttl=%s)", addr, service, ttl)
	}

	return &RedisRepository{client: client, service: service, ttl: ttl}
}

func ttlFromEnv() time.Duration {
	const def = 60 * time.Second
	v := os.Getenv("CACHE_TTL_SECONDS")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Printf("[cache] WARN: invalid CACHE_TTL_SECONDS=%q, using default %s", v, def)
		return def
	}
	return time.Duration(n) * time.Second
}

func (r *RedisRepository) key(entity, id string) string {
	return fmt.Sprintf("%s:%s:%s", r.service, entity, id)
}

func (r *RedisRepository) Get(ctx context.Context, entity, id string, dest any) error {
	if r == nil || r.client == nil {
		return ErrCacheMiss
	}
	data, err := r.client.Get(ctx, r.key(entity, id)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return ErrCacheMiss
		}
		log.Printf("[cache] WARN: get %s failed: %v", r.key(entity, id), err)
		return ErrCacheMiss
	}
	if err := json.Unmarshal(data, dest); err != nil {
		log.Printf("[cache] WARN: unmarshal %s failed: %v", r.key(entity, id), err)
		return ErrCacheMiss
	}
	return nil
}

func (r *RedisRepository) Set(ctx context.Context, entity, id string, value any) error {
	if r == nil || r.client == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		log.Printf("[cache] WARN: marshal %s failed: %v", r.key(entity, id), err)
		return nil
	}
	if err := r.client.Set(ctx, r.key(entity, id), data, r.ttl).Err(); err != nil {
		log.Printf("[cache] WARN: set %s failed: %v", r.key(entity, id), err)
		return nil
	}
	return nil
}

func (r *RedisRepository) Delete(ctx context.Context, entity, id string) error {
	if r == nil || r.client == nil {
		return nil
	}
	if err := r.client.Del(ctx, r.key(entity, id)).Err(); err != nil {
		log.Printf("[cache] WARN: delete %s failed: %v", r.key(entity, id), err)
		return nil
	}
	return nil
}

func (r *RedisRepository) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}
