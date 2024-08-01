// @Author xiaozhaofu 2023/7/17 20:45:00
package stream

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/vmihailenco/msgpack/v5"

	rdb "github.com/gtkit/redis"
)

// RedisStreamWrapper interface to handle streams
type RedisStreamWrapper interface {
	// SetChannels set the message and error channels
	SetChannels(messageChan chan interface{}, errChan chan error)
	// Publish data into the stream
	Publish(message interface{}) (string, error)
	// Consume messages from the stream with a count limit. If 0 it consumes all messages
	Consume(count int64)
	// MessageChannel get the message channel
	MessageChannel() chan interface{}
	// ErrorChannel get the error channel
	ErrorChannel() chan error
	// FinishedChannel get the finished notification channel
	FinishedChannel() chan bool
}

type redisStreamWrapper struct {
	c            *rdb.Redisclient
	stream       string
	bufferSize   int
	messageChan  chan interface{} // Channel where the consumed messages are send
	errChan      chan error
	finishedChan chan bool
}

func (s *redisStreamWrapper) client() *redis.Client {
	return s.c.Client()
}

func (s *redisStreamWrapper) ctx() context.Context {
	return s.c.Ctx()
}

// SetChannels set the message and error channels
func (s *redisStreamWrapper) SetChannels(messageChan chan interface{}, errChan chan error) {
	if messageChan != nil {
		s.messageChan = messageChan
	}
	if errChan != nil {
		s.errChan = errChan
	}
}

// MessageChannel get the message channel
func (s *redisStreamWrapper) MessageChannel() chan interface{} {
	return s.messageChan
}

// ErrorChannel get the error channel
func (s *redisStreamWrapper) ErrorChannel() chan error {
	return s.errChan
}

// FinishedChannel get the finished notification channel
func (s *redisStreamWrapper) FinishedChannel() chan bool {
	return s.finishedChan
}

// Publish data into the stream
func (s *redisStreamWrapper) Publish(message interface{}) (string, error) {
	args := redis.XAddArgs{
		Stream: s.stream,
		Values: map[string]interface{}{
			"data": message,
		},
	}
	return s.client().XAdd(s.ctx(), &args).Result()
}

// Consume messages from the stream with a count limit. If 0 it will consume all messages
func (s *redisStreamWrapper) Consume(count int64) {
	go func() {
		for {
			var err error
			var data []redis.XMessage
			if count > 0 {
				data, err = s.client().XRangeN(s.ctx(), s.stream, "-", "+", count).Result()
			} else {
				data, err = s.client().XRange(s.ctx(), s.stream, "-", "+").Result()
			}
			if err != nil {
				s.errChan <- err
			}

			for _, element := range data {
				data := []byte(element.Values["data"].(string)) // Get pack message
				var message interface{}
				err := msgpack.Unmarshal(data, &message)
				if err != nil {
					s.errChan <- err
					continue
				}
				s.messageChan <- message
				s.client().XDel(s.ctx(), s.stream, element.ID) // Remove consumed message
			}
		}
	}()
}
