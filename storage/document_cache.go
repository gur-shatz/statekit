package storage

import (
	"context"
	"time"

	"github.com/coocood/freecache"
	"github.com/klauspost/compress/zstd"
	"gopkg.in/yaml.v3"
)

type DocumentCache[T any] interface {
	Set(ctx context.Context, key string, value T, ttl time.Duration) error
	Get(ctx context.Context, key string) (T, bool, error)
	GetYAML(ctx context.Context, key string) ([]byte, bool, error)
}

type FreecacheDocumentCache[T any] struct {
	cache *freecache.Cache
}

func NewFreecacheDocumentCache[T any](sizeBytes int) *FreecacheDocumentCache[T] {
	return &FreecacheDocumentCache[T]{cache: freecache.NewCache(sizeBytes)}
}

func (c *FreecacheDocumentCache[T]) Set(_ context.Context, key string, value T, ttl time.Duration) error {
	data, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	encoded, err := encodeZstd(data)
	if err != nil {
		return err
	}
	return c.cache.Set([]byte(key), encoded, ttlSeconds(ttl))
}

func (c *FreecacheDocumentCache[T]) Get(ctx context.Context, key string) (T, bool, error) {
	var zero T
	data, ok, err := c.GetYAML(ctx, key)
	if err != nil || !ok {
		return zero, ok, err
	}
	var value T
	if err := yaml.Unmarshal(data, &value); err != nil {
		return zero, false, err
	}
	return value, true, nil
}

func (c *FreecacheDocumentCache[T]) GetYAML(_ context.Context, key string) ([]byte, bool, error) {
	value, err := c.cache.Get([]byte(key))
	if err == freecache.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	decoded, err := decodeZstd(value)
	if err != nil {
		return nil, false, err
	}
	return decoded, true, nil
}

func encodeZstd(data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil), nil
}

func decodeZstd(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	return decoder.DecodeAll(data, nil)
}

func ttlSeconds(ttl time.Duration) int {
	if ttl <= 0 {
		return 0
	}
	seconds := int(ttl.Seconds())
	if seconds < 1 {
		return 1
	}
	return seconds
}
