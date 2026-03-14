// Package redis 提供基于 github.com/redis/go-redis/v9 的生产级 Redis 客户端封装。
//
// v2 相比 v1 的主要改进:
//   - 无全局变量，支持多实例（多服务器/多配置）共存
//   - 全局 Key 前缀透明封装，业务层无感知（v1 需手动拼接）
//   - 支持 per-DB 独立前缀（兼容 v1 的 WithDB(0, "prefix") 用法）
//   - 完整连接池/超时配置（PoolSize、DialTimeout、ReadTimeout 等）
//   - SCAN 批量删除优化为 pipeline 批量 DEL（v1 逐个 DEL）
//   - NewClient 返回 error（v1 返回 nil）
//   - HealthCheck 健康检查 API
//   - Functional Options 使用 func(*Config) 模式，更简洁
//
// 快速使用:
//
//	c, err := redis.NewClient(
//	    redis.WithAddr("127.0.0.1:6379"),
//	    redis.WithPassword("123456"),
//	    redis.WithKeyPrefix("app:demo"),
//	    redis.WithInitDBs(0, 1, 2),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer c.Close()
//
//	ctx := context.Background()
//
//	// 默认 DB 操作，key 自动添加前缀 "app:demo:user:1"
//	c.Set(ctx, "user:1", "hello", 0)
//
//	// 切换 DB1 链式调用
//	val, _ := c.MustSelectDB(1).Get(ctx, "config").Result()
//
//	// SCAN 安全批量删除
//	deleted, _ := c.DelByPattern(ctx, "user:*")
package redis