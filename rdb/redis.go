// @Author xiaozhaofu 2022/11/23 00:23:00
package rdb

import (
	"context"
	"errors"
	"time"

	"github.com/gtkit/logger"
	"github.com/redis/go-redis/v9"
)

type Redisclient struct {
	client  *redis.Client
	context context.Context
	prefix  string // 前缀
}

// 使用redis 指定的单个库
func NewRedis(Addr, Password, Prefix string, db int) *Redisclient {
	rds := &Redisclient{}
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
		logger.Info("rds.client.Ping failed----", err)
		logger.Fatal(err)
		return nil
	}

	logger.Info("redis connect success db:", db, " --ping:", r)
	return rds
}

func initlogger() {
	if logger.Zlog() == nil {
		logger.NewZap(&logger.Option{
			FileStdout: true,
			Division:   "size",
		})
		logger.Info("--------- redis new zap logger -------------")
	}
}

func (r *Redisclient) Client() *redis.Client {
	return r.client
}
func (r *Redisclient) Ctx() context.Context {
	return r.context
}

// Set redis
func (r *Redisclient) Set(k string, v interface{}, exp time.Duration) bool {
	err := r.client.Set(r.context, r.prefix+k, v, exp).Err()
	if err != nil {
		logger.Info("redis set error:", err)
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
			logger.Infof("没有获取到redis %s的值：%s", r.prefix+k, err)

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

// Has 判断一个 key 是否存在，内部错误和 redis.Nil 都返回 false
func (r *Redisclient) Has(key string) bool {
	_, err := r.client.Exists(r.context, r.prefix+key).Result()
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
func (r *Redisclient) Expire(key string, dur time.Duration) bool {
	err := r.client.Expire(r.context, r.prefix+key, dur).Err()
	if err != nil {
		logger.Info("Expire error:", err)
		return false
	}
	return true
}

// 判断key是不是存在
func (r *Redisclient) Exists(keys string) bool {
	val, err := r.client.Get(r.context, r.prefix+keys).Result()
	if err != nil {
		return false
	}
	if val == "" || err == redis.Nil {
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

func (r *Redisclient) Spop(k string) (string, error) {
	rs, err := r.client.SPop(r.context, r.prefix+k).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// logger.Infof("没有获取到redis的值：%s", err)
			return "", nil
		}
		return "", err
	}
	return rs, nil
}

func (r *Redisclient) Lpop(k string) (string, error) {
	rs, err := r.client.LPop(r.context, r.prefix+k).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// logger.Infof("没有获取到redis的值：%s", err)
			return "", nil
		}
		return "", err
	}
	return rs, nil
}

func (r *Redisclient) Lpush(k string, val ...interface{}) (int64, error) {
	rs, err := r.client.LPush(r.context, r.prefix+k, val).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// logger.Infof("没有获取到redis的值：%s", err)
			return 0, nil
		}
		return 0, err
	}
	return rs, nil
}

func (r *Redisclient) Rpop(k string) (string, error) {
	rs, err := r.client.RPop(r.context, r.prefix+k).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// logger.Infof("没有获取到redis的值：%s", err)
			return "", nil
		}
		return "", err
	}
	return rs, nil
}

func (r *Redisclient) Rpush(k string, val ...interface{}) (int64, error) {
	rs, err := r.client.RPush(r.context, r.prefix+k, val).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// logger.Infof("没有获取到redis的值：%s", err)
			return 0, nil
		}
		return 0, err
	}
	return rs, nil
}

func (r *Redisclient) Llen(k string) (int64, error) {
	rs, err := r.client.LLen(r.context, r.prefix+k).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// logger.Infof("没有获取到redis的值：%s", err)
			return 0, nil
		}
		return 0, err
	}
	return rs, nil
}

// Lrem 移除列表中与参数 value 相等的元素
func (r *Redisclient) Lrem(k string, count int64, val interface{}) (int64, error) {
	rs, err := r.client.LRem(r.context, r.prefix+k, count, val).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return rs, nil
}

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
