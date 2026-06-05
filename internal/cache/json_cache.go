package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type JSONCache struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewJSONCache(rdb *redis.Client, ttl time.Duration) *JSONCache {
	return &JSONCache{rdb: rdb, ttl: ttl}
}

func (c *JSONCache) GetOrSet(ctx context.Context, key string, dest any, load func() (any, error)) error {
	if c == nil || c.rdb == nil {
		v, err := load()
		if err != nil {
			return err
		}
		return copyViaJSON(dest, v)
	}
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err == nil {
		return json.Unmarshal(raw, dest)
	}
	if err != redis.Nil {
		v, loadErr := load()
		if loadErr != nil {
			return loadErr
		}
		return copyViaJSON(dest, v)
	}
	v, err := load()
	if err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return copyViaJSON(dest, v)
	}
	_ = c.rdb.Set(ctx, key, b, c.ttl).Err()
	return json.Unmarshal(b, dest)
}

func (c *JSONCache) Delete(ctx context.Context, keys ...string) {
	if c == nil || c.rdb == nil || len(keys) == 0 {
		return
	}
	_ = c.rdb.Del(ctx, keys...).Err()
}

func copyViaJSON(dest, src any) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dest)
}
