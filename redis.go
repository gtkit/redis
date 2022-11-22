// @Author xiaozhaofu 2022/11/23 00:23:00
package rdb

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisClient struct {
	Client  *redis.Client
	Context context.Context
}

func NewRedis(Addr string, Password string, rdb int) (*RedisClient) {
	// 初始化自定的 RedisClient 实例
	rds := &RedisClient{}

	rds.Context = context.Background()
	rds.Client = redis.NewClient(&redis.Options{
		Addr:     Addr,
		Password: Password,
		DB:       rdb,
	})
	// 测试链接
	r, err := rds.Client.Ping(rds.Context).Result()
	if err != nil {
		logger.Fatal(err)
	}

	logger.Info("redis connect success!", r)
	return rds
}

// Get 读取 redis
func (r *RedisClient) Get(k string) string {
	rs, err := r.Client.Get(r.Context, k).Result()
	if err != nil {
		// 如果返回的错误是key不存在
		if errors.Is(err, redis.Nil) {
			logger.Infof("没有获取到redis的值：%s", err)

		}
		return ""
	}
	return rs
}

// Set redis
func (r *RedisClient) Set(k, v string, exp time.Duration) bool {
	err := r.Client.Set(r.Context, k, v, exp).Err()
	return err == nil
}

// Has 判断一个 key 是否存在，内部错误和 redis.Nil 都返回 false
func (r *RedisClient) Has(key string) bool {
	_, err := r.Client.Get(r.Context, key).Result()
	if err != nil {
		if err != redis.Nil {
			logger.Error("Redis", "Has", err.Error())
		}
		return false
	}
	return true
}

// Del 删除存储在 redis 里的数据，支持多个 key 传参
func (r *RedisClient) Del(keys ...string) bool {
	if err := r.Client.Del(r.Context, keys...).Err(); err != nil {
		logger.Error("Redis", "Del", err.Error())
		return false
	}
	return true
}

// Select 选择指定的 db
func (r *RedisClient) Select(db int) {
	_, err := r.Client.Pipelined(r.Context, func(pipeliner redis.Pipeliner) error {
		pipeliner.Select(r.Context, db)
		return nil
	})

	if err != nil {
		panic(err)
	}

}
