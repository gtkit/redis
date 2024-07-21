package redis

import (
	"errors"

	"github.com/redis/go-redis/v9"
)

func (r *Redisclient) Hexists(key, field string) (bool, error) {
	rs, err := r.client.HExists(r.context, r.prefix+key, field).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	return rs, nil
}

func (r *Redisclient) Hincrby(key, field string, incr int64) (int64, error) {
	rs, err := r.client.HIncrBy(r.context, r.prefix+key, field, incr).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return rs, nil
}

func (r *Redisclient) Hset(key string, val ...any) (int64, error) {
	rs, err := r.client.HSet(r.context, r.prefix+key, val).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return rs, nil
}

func (r *Redisclient) Hget(key, field string) (string, error) {
	rs, err := r.client.HGet(r.context, r.prefix+key, field).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return rs, nil
}
