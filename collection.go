package redis

import (
	"sync"
)

type DBConfig struct {
	Prefix string
	DB     int
}

// ClientConfig redis 链接配置信息
type ClientConfig struct {
	Addr       string
	UserName   string
	Password   string
	ClientName string
	DBConfig
}

//go:generate go-option -type ConnConfig
type ConnConfig struct {
	_          [0]func() // 占位符，防止被其他包引用
	addr       string
	username   string
	password   string
	clientname string
	dbconfig   []DBConfig
}

// redisConfigs 分组配置信息
type redisConfigs map[int]*ClientConfig

// once 确保全局Redis对象只实例一次
var once sync.Once

// redisCollections redis对象集合
var redisCollections map[int]*Redisclient

// 使用redis 多个库
// func NewCollection(addr, username, password string, dbconf []dbConfig) map[int]*Redisclient {
func NewCollection(opts ...ConnConfigOption) map[int]*Redisclient {
	connectRedis(setredisConfigs(opts...))
	return redisCollections
}

// Select 获取指定库的 Redis 对象
func Select(db int) *Redisclient {
	if redisCollections == nil {
		return nil
	}

	if rdb, ok := redisCollections[db]; ok {
		return rdb
	}
	return nil
}

func setredisConfigs(opts ...ConnConfigOption) redisConfigs {
	redisConfigs := make(redisConfigs)
	conf := NewConnConfig(opts...)

	for _, dc := range conf.dbconfig {
		redisConfigs[dc.DB] = &ClientConfig{
			conf.addr,
			conf.username,
			conf.password,
			conf.clientname,
			DBConfig{
				dc.Prefix,
				dc.DB,
			},
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
			redisCollections[dbname] = New(rdbconfig)
		}
	})
}
