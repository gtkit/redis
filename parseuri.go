package redis

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/gtkit/redis/failover"
)

// ParseRedisURI parses redis uri string and returns RedisConnOpt if uri is valid.
// It returns a non-nil error if uri cannot be parsed.
//
// Three URI schemes are supported, which are redis:, rediss:, redis-socket:, and redis-sentinel:.
// Supported formats are:
//
//	redis://[:password@]host[:port][/dbnumber]
//	rediss://[:password@]host[:port][/dbnumber]
//	redis-socket://[:password@]path[?db=dbnumber]
//	redis-sentinel://[:password@]host1[:port][,host2:[:port]][,hostN:[:port]][?master=masterName]
func ParseRedisURI(uri string) (RedisConnOpt, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("asynq: could not parse redis uri: %v", err)
	}
	switch u.Scheme {
	case "redis", "rediss":
		return parseRedisURI(u)
	case "redis-socket":
		return parseRedisSocketURI(u)
	case "redis-sentinel":
		return parseRedisSentinelURI(u)
	default:
		return nil, fmt.Errorf("asynq: unsupported uri scheme: %q", u.Scheme)
	}
}

func parseRedisURI(u *url.URL) (RedisConnOpt, error) {
	var db int
	var err error
	var redisConnOpt RedisClientOpt

	if len(u.Path) > 0 {
		xs := strings.Split(strings.Trim(u.Path, "/"), "/")
		db, err = strconv.Atoi(xs[0])
		if err != nil {
			return nil, fmt.Errorf("asynq: could not parse redis uri: database number should be the first segment of the path")
		}
	}
	var password string
	if v, ok := u.User.Password(); ok {
		password = v
	}

	if u.Scheme == "rediss" {
		h, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			h = u.Host
		}
		redisConnOpt.TLSConfig = &tls.Config{ServerName: h}
	}

	redisConnOpt.Addr = u.Host
	redisConnOpt.Password = password
	redisConnOpt.DB = db

	return redisConnOpt, nil
}

func parseRedisSocketURI(u *url.URL) (RedisConnOpt, error) {
	const errPrefix = "asynq: could not parse redis socket uri"
	if len(u.Path) == 0 {
		return nil, fmt.Errorf("%s: path does not exist", errPrefix)
	}
	q := u.Query()
	var db int
	var err error
	if n := q.Get("db"); n != "" {
		db, err = strconv.Atoi(n)
		if err != nil {
			return nil, fmt.Errorf("%s: query param `db` should be a number", errPrefix)
		}
	}
	var password string
	if v, ok := u.User.Password(); ok {
		password = v
	}
	return RedisClientOpt{Network: "unix", Addr: u.Path, DB: db, Password: password}, nil
}

func parseRedisSentinelURI(u *url.URL) (RedisConnOpt, error) {
	addrs := strings.Split(u.Host, ",")
	master := u.Query().Get("master")
	var password string
	if v, ok := u.User.Password(); ok {
		password = v
	}
	return failover.RedisFailoverClientOpt{MasterName: master, SentinelAddrs: addrs, SentinelPassword: password}, nil
}
