// @Author xiaozhaofu 2022/11/24 02:10:00
package cdb

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"gitlab.superjq.com/go-tools/logger"
)

type CacheClient struct {
	client  *redis.Client
	context context.Context
	prefix  string // 前缀
}

var cache = &CacheClient{}

func NewCache(Addr, Password, Prefix string, cdb int) *CacheClient {
	// 初始化日志
	initlogger()
	// 初始化自定的 RedisClient 实例
	cache.prefix = Prefix
	cache.context = context.Background()
	cache.client = redis.NewClient(&redis.Options{
		Addr:     Addr,
		Password: Password,
		DB:       cdb,
	})
	// 测试链接
	c, err := cache.client.Ping(cache.context).Result()
	if err != nil {
		logger.Fatal(err)
	}

	logger.Info("cache init success!", c)
	return cache
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

func (c *CacheClient) Client() *redis.Client {
	return c.client
}
func (c *CacheClient) Context() context.Context {
	return c.context
}
func (c *CacheClient) Prefix() string {
	return c.prefix
}

// Get 读取 redis
func (c *CacheClient) Get(k string) string {
	rs, err := c.client.Get(c.context, c.prefix+k).Result()
	if err != nil {
		// 如果返回的错误是key不存在
		if errors.Is(err, redis.Nil) {
			// logger.Infof("没有获取到redis的值：%s", err)
			return ""
		}
		return ""
	}
	return rs
}

// Set redis
func (c *CacheClient) Set(k string, v interface{}, exp time.Duration) bool {
	err := c.client.Set(c.context, c.prefix+k, v, exp).Err()
	if err != nil {
		// panic(err)
		logger.Error("cache set error:", err)
		return false
	}
	return true
}

// Has 判断一个 key 是否存在，内部错误和 redis.Nil 都返回 false
func (c *CacheClient) Has(key string) bool {
	_, err := c.client.Get(c.context, c.prefix+key).Result()
	if err != nil {
		if err != redis.Nil {
			logger.Error("Redis", "Has", err.Error())
		}
		return false
	}
	return true
}
