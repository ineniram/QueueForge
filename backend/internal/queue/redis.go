package queue

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"queueforge/internal/jobs"
)

type Queue struct {
	client *redis.Client
	key    string
}

func Open(url, key string) (*Queue, error) {
	config, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	config.ContextTimeoutEnabled = true
	// A retried blocking pop could remove a second job after a lost response.
	config.MaxRetries = -1
	return &Queue{redis.NewClient(config), key}, nil
}

func (q *Queue) Close() error                   { return q.client.Close() }
func (q *Queue) Ping(ctx context.Context) error { return q.client.Ping(ctx).Err() }

func (q *Queue) Publish(ctx context.Context, refs []jobs.Ref) error {
	if len(refs) == 0 {
		return nil
	}
	members := make([]redis.Z, 0, len(refs))
	for _, ref := range refs {
		members = append(members, redis.Z{Score: float64(ref.CreatedAt.UnixMicro()), Member: ref.ID})
	}
	return q.client.ZAddNX(ctx, q.key, members...).Err()
}

func (q *Queue) Pop(ctx context.Context) (string, error) {
	result, err := q.client.BZPopMin(ctx, time.Second, q.key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	id, ok := result.Member.(string)
	if !ok {
		return "", errors.New("Redis returned a non-string job ID")
	}
	return id, nil
}
