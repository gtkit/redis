package redis

import (
	"context"

	"github.com/gtkit/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Redisclient struct {
	client *redis.Client
	prefix string // 前缀
}

// New 使用redis 指定的单个库.
func New(conf *ClientConfig) *Redisclient {
	rds := &Redisclient{}
	// 初始化日志
	initlogger()
	// 初始化自定的 redisclient 实例
	rds.prefix = conf.Prefix + ":"
	rds.client = redis.NewClient(&redis.Options{
		Addr:     conf.Addr,
		Username: conf.UserName,
		Password: conf.Password,
		DB:       conf.DB,
	})

	// 测试链接
	r, err := rds.client.Ping(context.Background()).Result()
	if err != nil {
		logger.ZError("redis connect failed", zap.Error(err))
		return nil
	}

	logger.ZInfo("redis connect success", zap.Int("db", conf.DB), zap.String("ping", r))
	return rds
}

func initlogger() {
	if logger.Zlog() == nil {
		logger.NewZap(logger.WithFile(true), logger.WithConsole(true))
		logger.ZInfo("redis new zap logger")
	}
}

// Client 返回redis的client实例.
func (r *Redisclient) Client() *redis.Client {
	return r.client
}

// Close 关闭redis链接.
func (r *Redisclient) Close() error {
	return r.client.Close()
}

// Prefix 返回redis的前缀.
func (r *Redisclient) Prefix() string {
	return r.prefix
}


// BatchDel 批量删除redis中匹配的key.
//
// match: 匹配的key 如: "user:*".
func (r *Redisclient) BatchDel(ctx context.Context,match string) {
	iter := r.client.Scan(ctx, 0, match, 0).Iterator()
	if err := iter.Err(); err != nil {
		logger.Info("scan keys err: ", err)
		return
	}

	for iter.Next(ctx) {
		val := iter.Val()
		err := r.client.Del(ctx, val).Err()
		logger.Info("--- del key ---", val)
		if err != nil {
			logger.Info("del key err: ", err)
			continue
		}
	}
}
