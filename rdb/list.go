package rdb

import (
	"errors"

	"github.com/redis/go-redis/v9"
)

// Llen 获取列表长度
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
