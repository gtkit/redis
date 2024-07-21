package cluster

import (
	"crypto/tls"
	"time"

	"github.com/redis/go-redis/v9"
)

// ClientOpt is used to creates a redis client that connects to
// redis cluster.
//
//go:generate go-option -type ClientOpt
type ClientOpt struct {
	// A seed list of host:port addresses of cluster nodes.
	Addrs []string

	// The maximum number of retries before giving up.
	// Command is retried on network errors and MOVED/ASK redirects.
	// Default is 8 retries.
	MaxRedirects int

	// Username to authenticate the current connection when Redis ACLs are used.
	// See: https://redis.io/commands/auth.
	Username string

	// Password to authenticate the current connection.
	// See: https://redis.io/commands/auth.
	Password string

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration

	// Timeout for socket reads.
	// If timeout is reached, read commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	ReadTimeout time.Duration

	// Timeout for socket writes.
	// If timeout is reached, write commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is ReadTimeout.
	WriteTimeout time.Duration

	// TLS Config used to connect to a server.
	// TLS will be negotiated only if this field is set.
	TLSConfig *tls.Config
}

func (opt ClientOpt) MakeClient() any {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        opt.Addrs,
		MaxRedirects: opt.MaxRedirects,
		Username:     opt.Username,
		Password:     opt.Password,
		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,
		TLSConfig:    opt.TLSConfig,
	})
}
