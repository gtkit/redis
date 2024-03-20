package rdb

import (
	"errors"

	"github.com/gtkit/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Ismember 判断是不是集和中的元素
func (r *Redisclient) Ismember(key string, member interface{}) bool {
	ism, err := r.client.SIsMember(r.context, r.prefix+key, member).Result()
	if err != nil {
		logger.ZError("SIsMember error", zap.Error(err))
		return false
	}
	return ism
}

// SIsMember 判断是不是集和中的元素
func (r *Redisclient) SIsMember(key string, member interface{}) bool {
	ism, err := r.client.SIsMember(r.context, r.prefix+key, member).Result()
	if err != nil {
		logger.ZError("SIsMember error", zap.Error(err))
		return false
	}
	return ism
}

// SAdd 添加集合元素
func (r *Redisclient) SAdd(key string, members ...interface{}) bool {
	_, err := r.client.SAdd(r.context, r.prefix+key, members).Result()
	if err != nil {
		return false
	}
	return true
}

// SMembers 获取集合中的元素
func (r *Redisclient) SMembers(key string, res any) (any, error) {
	if err := r.client.SMembers(r.context, r.prefix+key).ScanSlice(res); err != nil {
		return nil, err
	}
	return res, nil
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
