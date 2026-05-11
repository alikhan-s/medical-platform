package middleware

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	defaultRPM    = 100
	slidingWindow = time.Minute
)

type RateLimiter struct {
	client *redis.Client
	rpm    int64
	prefix string
}

func NewRateLimiter(addr, servicePrefix string) *RateLimiter {
	rpm := rpmFromEnv()
	client := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("[ratelimit] WARN: redis ping failed at %s: %v (continuing — limiter will fail-open)", addr, err)
	} else {
		log.Printf("[ratelimit] connected to redis at %s (service=%s, rpm=%d)", addr, servicePrefix, rpm)
	}

	return &RateLimiter{client: client, rpm: int64(rpm), prefix: servicePrefix}
}

func rpmFromEnv() int {
	v := os.Getenv("RATE_LIMIT_RPM")
	if v == "" {
		return defaultRPM
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Printf("[ratelimit] WARN: invalid RATE_LIMIT_RPM=%q, using default %d", v, defaultRPM)
		return defaultRPM
	}
	return n
}

func (rl *RateLimiter) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ip := clientIP(ctx)
		if err := rl.allow(ctx, ip); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (rl *RateLimiter) allow(ctx context.Context, ip string) error {
	if rl == nil || rl.client == nil {
		return nil
	}

	key := rl.prefix + ":ratelimit:" + ip
	now := time.Now()
	nowNs := now.UnixNano()
	windowStart := now.Add(-slidingWindow).UnixNano()

	pipe := rl.client.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "0", strconv.FormatInt(windowStart, 10))
	countCmd := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("[ratelimit] WARN: redis pipeline failed for %s: %v (fail-open)", key, err)
		return nil
	}

	if countCmd.Val() >= rl.rpm {
		return status.Errorf(codes.ResourceExhausted,
			"rate limit exceeded: max %d requests per minute", rl.rpm)
	}

	member := strconv.FormatInt(nowNs, 10)
	if err := rl.client.ZAdd(ctx, key, redis.Z{Score: float64(nowNs), Member: member}).Err(); err != nil {
		log.Printf("[ratelimit] WARN: redis ZADD failed for %s: %v", key, err)
		return nil
	}
	rl.client.Expire(ctx, key, slidingWindow*2)
	return nil
}

func clientIP(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p == nil || p.Addr == nil {
		return "unknown"
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String()
	}
	return host
}

func (rl *RateLimiter) Close() error {
	if rl == nil || rl.client == nil {
		return nil
	}
	return rl.client.Close()
}
