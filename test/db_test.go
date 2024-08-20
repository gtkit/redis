package test

import (
	"context"
	"testing"

	"github.com/gtkit/redis"
)

func TestConn(t *testing.T) {
	// conf := &redis.ClientConfig{
	// 	Addr:     "127.0.0.1:6379",
	// 	UserName: "",
	// 	Password: "",
	// 	DBConfig: redis.DBConfig{
	// 		DB:     0,
	// 		Prefix: "test",
	// 	},
	// }
	// redis.New(conf)

	conn := redis.NewCollection(
		redis.WithAddr("127.0.0.1:6379"),
		redis.WithDB(0, "test"),
		redis.WithDB(1),
		redis.WithDB(2, "prefix:test2"),
	)
	c := conn[0].Client()
	c.Ping(context.Background())
	rdb := redis.Select(2)
	rdb.Client().Set(context.Background(), rdb.Prefix()+"key:2", "value:2", 0)

}
