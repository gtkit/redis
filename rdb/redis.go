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
	prefix  string // 前缀
}

var rds = &Redisclient{}

func NewRedis(Addr, Password, Prefix string, db int) *Redisclient {
	// 初始化日志
	initlogger()
	// 初始化自定的 redisclient 实例

	rds.prefix = Prefix
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

func (r *Redisclient) Client() *redis.Client {
	return r.client
}

// Get 读取 redis
func (r *Redisclient) Get(k string) string {
	rs, err := r.client.Get(r.context, r.prefix+k).Result()
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
	err := r.client.Set(r.context, r.prefix+k, v, exp).Err()
	return err == nil
}

// Has 判断一个 key 是否存在，内部错误和 redis.Nil 都返回 false
func (r *Redisclient) Has(key string) bool {
	_, err := r.client.Get(r.context, r.prefix+key).Result()
	if err != nil {
		if err != redis.Nil {
			logger.Error("Redis", "Has", err.Error())
		}
		return false
	}
	return true
}

// 判断是不是集和中的元素
func (r *Redisclient) Ismember(key string, member interface{}) bool {
	ism, err := r.client.SIsMember(r.context, r.prefix+key, member).Result()
	if err != nil {
		logger.Info("SIsMember error:", err)
		return false
	}
	return ism
}

// 添加集合元素
func (r *Redisclient) SAdd(key string, members ...interface{}) bool {
	_, err := r.client.SAdd(r.context, r.prefix+key, members).Result()
	if err != nil {
		return false
	}
	return true
}

// 设置key过期时间
func (r *Redisclient) Expire(key string, duration time.Duration) bool {
	b, err := r.client.Expire(r.context, r.prefix+key, duration).Result()
	if err != nil {
		return false
	}
	return b
}

// 判断key是不是存在
func (r *Redisclient) Exists(keys string) bool {
	_, err := r.client.Exists(r.context, r.prefix+keys).Result()
	if err != nil {
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

// Increment 当参数只有 1 个时，为 key，其值增加 1。
// 当参数有 2 个时，第一个参数为 key ，第二个参数为要增加的值 int64 类型。
func (r *Redisclient) Incr(parameters ...interface{}) bool {
	switch len(parameters) {
	case 1:
		key := parameters[0].(string)
		if err := r.client.Incr(r.context, r.prefix+key).Err(); err != nil {
			logger.Error("Redis key1 ", err)
			return false
		}
	case 2:
		key := parameters[0].(string)
		value := parameters[1].(int64)
		if err := r.client.IncrBy(r.context, r.prefix+key, value).Err(); err != nil {
			logger.Error("Redis key2 ", err)
			return false
		}
	default:
		logger.Error("Redis parameters error")
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
