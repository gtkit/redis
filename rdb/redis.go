// @Author xiaozhaofu 2022/11/23 00:23:00
package rdb

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"gitlab.superjq.com/go-tools/logger"
)

type Redisclient struct {
	client  *redis.Client
	context context.Context
}

var rds = &Redisclient{}

func NewRedis(Addr string, Password string, db int) *Redisclient {
	// 初始化日志
	initlogger()
	// 初始化自定的 redisclient 实例

	rds.context = context.Background()
	rds.client = redis.NewClient(&redis.Options{
		Addr:     Addr,
		Password: Password,
		DB:       db,
	})
	// 测试链接
	r, err := rds.client.Ping(rds.context).Result()
	if err != nil {
		logger.Fatal(err)
	}

	logger.Info("redis connect success!", r)
	return rds
}

func Client() *redis.Client {
	return rds.client
}

func initlogger() {
	if logger.Zlog() == nil {
		opt := &logger.Option{
			FileStdout: true,
			Division:   "size",
		}
		logger.NewZap(opt)
		log.Println("redis new zap logger")
	}
}

// Get 读取 redis
func (r *Redisclient) Get(k string) string {
	rs, err := r.client.Get(r.context, k).Result()
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
func (r *Redisclient) Set(k, v string, exp time.Duration) bool {
	err := r.client.Set(r.context, k, v, exp).Err()
	return err == nil
}

// Has 判断一个 key 是否存在，内部错误和 redis.Nil 都返回 false
func (r *Redisclient) Has(key string) bool {
	_, err := r.client.Get(r.context, key).Result()
	if err != nil {
		if err != redis.Nil {
			logger.Error("Redis", "Has", err.Error())
		}
		return false
	}
	return true
}

// Del 删除存储在 redis 里的数据，支持多个 key 传参
func (r *Redisclient) Del(keys ...string) bool {
	if err := r.client.Del(r.context, keys...).Err(); err != nil {
		logger.Error("Redis", "Del", err.Error())
		return false
	}
	return true
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
