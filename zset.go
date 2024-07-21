package redis

import (
	"github.com/redis/go-redis/v9"
)

// ZAdd 添加集合元素
func (r *Redisclient) ZAdd(key string, members ...redis.Z) bool {
	_, err := r.client.ZAdd(r.context, r.prefix+key, redis.Z{Score: 0, Member: members[0]}).Result()
	if err != nil {
		return false
	}
	return true
}
