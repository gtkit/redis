// @Author xiaozhaofu 2023/2/24 20:24:00
package rdb

import (
	"sync"
)

// redisClientConfig redis 链接配置信息
type redisClientConfig struct {
	host     string
	password string
	prefix   string
	db       int
}

// redisConfigs 分组配置信息
type redisConfigs map[int]*redisClientConfig

// once 确保全局Redis对象只实例一次
var once sync.Once

// redisCollections redis对象集合
var redisCollections map[int]*Redisclient

// 使用redis 多个库
func NewRedisCollection(addr, password, prefix string, dbs []int) map[int]*Redisclient {
	redisConfigs := setredisConfigs(addr, password, prefix, dbs)
	connectRedis(redisConfigs)
	return redisCollections
}

func setredisConfigs(addr, password, prefix string, dbs []int) redisConfigs {
	redisConfigs := make(redisConfigs)

	for _, db := range dbs {
		redisConfigs[db] = &redisClientConfig{
			addr,
			password,
			prefix,
			db,
		}
	}

	return redisConfigs
}

// ConnectRedis 连接 redis 数据库，设置全局的 Redis 对象
func connectRedis(configs redisConfigs) {
	once.Do(func() {
		if redisCollections == nil {
			redisCollections = make(map[int]*Redisclient, len(redisCollections))
		}

		for dbname, rdbconfig := range configs {
			redisCollections[dbname] = NewRedis(rdbconfig.host, rdbconfig.password, rdbconfig.prefix, rdbconfig.db)
		}
	})
}
