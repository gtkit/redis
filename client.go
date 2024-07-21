package redis

import (
	"context"

	"github.com/gtkit/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Redisclient struct {
	client  *redis.Client
	context context.Context
	prefix  string // 前缀
}

func New() {
	//rds := &Redisclient{}
	// 初始化日志
	initlogger()
	// 初始化自定的 redisclient 实例

}

// 使用redis 指定的单个库
func NewRedis(Addr, Username, Password, Prefix string, db int) *Redisclient {
	rds := &Redisclient{}
	// 初始化日志
	initlogger()
	// 初始化自定的 redisclient 实例

	rds.prefix = Prefix
	rds.context = context.Background()
	rds.client = redis.NewClient(&redis.Options{
		Addr:     Addr,
		Username: Username,
		Password: Password,
		DB:       db,
	})

	// 测试链接
	r, err := rds.client.Ping(rds.context).Result()
	if err != nil {
		logger.ZError("redis connect failed", zap.Error(err))
		return nil
	}

	logger.ZInfo("redis connect success", zap.Int("db", db), zap.String("ping", r))
	return rds
}

func initlogger() {
	if logger.Zlog() == nil {
		logger.NewZap(logger.WithFile(true))
		logger.ZInfo("redis new zap logger")
	}
}

func (r *Redisclient) Client() *redis.Client {
	return r.client
}
func (r *Redisclient) Ctx() context.Context {
	return r.context
}

func (r *Redisclient) Close() error {
	return r.client.Close()
}
