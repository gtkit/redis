package redis

import (
	"errors"
	"time"

	"github.com/gtkit/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Set redis
func (r *Redisclient) Set(k string, v interface{}, exp time.Duration) bool {
	err := r.client.Set(r.context, r.prefix+k, v, exp).Err()
	if err != nil {
		logger.ZError("redis set error ", zap.Error(err))
		return false
	}
	return err == nil
}

// Get 读取 redis
func (r *Redisclient) Get(k string) string {
	rs, err := r.client.Get(r.context, r.prefix+k).Result()
	if err != nil {
		// 如果返回的错误是key不存在
		if errors.Is(err, redis.Nil) {
			logger.ZError("没有获取到redis值:", zap.Error(err))
		}
		return ""
	}
	return rs
}
func (r *Redisclient) GetBytes(k string) []byte {
	rs, err := r.client.Get(r.context, r.prefix+k).Bytes()
	if err != nil {
		// 如果返回的错误是key不存在
		if errors.Is(err, redis.Nil) {
			logger.Infof("没有获取到redis %s的值：%s", r.prefix+k, err)

		}
		return nil
	}
	return rs
}
func (r *Redisclient) GetInt(k string) int {
	rs, err := r.client.Get(r.context, r.prefix+k).Int()
	if err != nil {
		// 如果返回的错误是key不存在
		if errors.Is(err, redis.Nil) {
			logger.Infof("没有获取到redis %s的值：%s", r.prefix+k, err)

		}
		return 0
	}
	return rs
}
func (r *Redisclient) GetInt64(k string) int64 {
	rs, err := r.client.Get(r.context, r.prefix+k).Int64()
	if err != nil {
		// 如果返回的错误是key不存在
		if errors.Is(err, redis.Nil) {
			logger.Infof("没有获取到redis %s的值：%s", r.prefix+k, err)

		}
		return 0
	}
	return rs
}

func (r *Redisclient) GetUint64(k string) uint64 {
	rs, err := r.client.Get(r.context, r.prefix+k).Uint64()
	if err != nil {
		// 如果返回的错误是key不存在
		if errors.Is(err, redis.Nil) {
			logger.Infof("没有获取到redis %s的值：%s", r.prefix+k, err)

		}
		return 0
	}
	return rs
}

// 设置key过期时间
func (r *Redisclient) Expire(key string, dur time.Duration) bool {
	err := r.client.Expire(r.context, r.prefix+key, dur).Err()
	if err != nil {
		logger.ZError("Expire error ", zap.Error(err))
		return false
	}
	return true
}

// 判断key是不是存在
func (r *Redisclient) Exists(keys string) bool {
	val := r.client.Exists(r.context, r.prefix+keys).Val()
	if val == 0 {
		return false
	}
	return true
}

// Has 判断一个 key 是否存在，内部错误和 redis.Nil 都返回 false
func (r *Redisclient) Has(key string) bool {
	val := r.client.Exists(r.context, r.prefix+key).Val()
	if val == 0 {
		return false
	}
	return true
}

// Del 删除存储在 redis 里的数据，支持多个 key 传参
func (r *Redisclient) Del(keys ...string) bool {
	if len(keys) == 0 {
		return false
	}
	var prekeys []string
	for _, k := range keys {
		prekeys = append(prekeys, r.prefix+k)
	}
	if err := r.client.Del(r.context, prekeys...).Err(); err != nil {
		logger.ZError("Redis Del ", zap.Error(err))
		return false
	}
	return true
}

// Incr 当参数只有 1 个时，为 key，其值增加 1。
// 当参数有 2 个时，第一个参数为 key ，第二个参数为要增加的值 int64 类型。
func (r *Redisclient) Incr(parameters ...interface{}) bool {
	switch len(parameters) {
	case 1:
		key := parameters[0].(string)
		if err := r.client.Incr(r.context, r.prefix+key).Err(); err != nil {
			logger.ZError("Redis key1 ", zap.Error(err))
			return false
		}
	case 2:
		key := parameters[0].(string)
		value := parameters[1].(int64)
		if err := r.client.IncrBy(r.context, r.prefix+key, value).Err(); err != nil {
			logger.ZError("Redis key2 ", zap.Error(err))
			return false
		}
	default:
		logger.ZError("Redis parameters error")
		return false
	}
	return true
}

func (r *Redisclient) TTL(key string) (time.Duration, error) {
	rs, err := r.client.TTL(r.context, r.prefix+key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return rs, nil
}

// Select 选择指定的 db
func (r *Redisclient) Select(db int) {
	_, err := r.client.Pipelined(r.context, func(pipeliner redis.Pipeliner) error {
		pipeliner.Select(r.context, db)
		return nil
	})

	if err != nil {
		panic(err)
	}

}
