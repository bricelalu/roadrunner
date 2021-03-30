package redis

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/go-redis/redis/v8"
	"github.com/spiral/roadrunner/v2/plugins/broadcast"
)

// Redis based broadcast Router.
type Redis struct {
	client        redis.UniversalClient
	psClient      redis.UniversalClient
	router        *broadcast.Router
	messages      chan *broadcast.Message
	listen, leave chan broadcast.Subscriber
	stop          chan interface{}
	stopped       int32
}

// creates new redis broker
func redisBroker(cfg *RedisConfig) (*Redis, error) {
	client := cfg.RedisClient()
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}

	psClient := cfg.RedisClient()
	if _, err := psClient.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}

	return &Redis{
		client:   client,
		psClient: psClient,
		router:   broadcast.NewRouter(),
		messages: make(chan *broadcast.Message),
		listen:   make(chan broadcast.Subscriber),
		leave:    make(chan broadcast.Subscriber),
		stop:     make(chan interface{}),
		stopped:  0,
	}, nil
}

// Serve serves broker.
func (r *Redis) Serve() error {
	pubsub := r.psClient.Subscribe(context.Background())
	channel := pubsub.Channel()

	for {
		select {
		case ctx := <-r.listen:
			ctx.Done <- r.handleJoin(ctx, pubsub)
		case ctx := <-r.leave:
			ctx.Done <- r.handleLeave(ctx, pubsub)
		case msg := <-channel:
			r.router.Dispatch(&broadcast.Message{
				Topic:   msg.Channel,
				Payload: []byte(msg.Payload),
			})
		case <-r.stop:
			return nil
		}
	}
}

func (r *Redis) handleJoin(sub broadcast.Subscriber, pubsub *redis.PubSub) error {
	if sub.Pattern != "" {
		newPatterns, err := r.router.SubscribePattern(sub.Upstream, sub.Pattern)
		if err != nil || len(newPatterns) == 0 {
			return err
		}

		return pubsub.PSubscribe(context.Background(), newPatterns...)
	}

	newTopics := r.router.Subscribe(sub.Upstream, sub.Topics...)
	if len(newTopics) == 0 {
		return nil
	}

	return pubsub.Subscribe(context.Background(), newTopics...)
}

func (r *Redis) handleLeave(sub broadcast.Subscriber, pubsub *redis.PubSub) error {
	if sub.Pattern != "" {
		dropPatterns := r.router.UnsubscribePattern(sub.Upstream, sub.Pattern)
		if len(dropPatterns) == 0 {
			return nil
		}

		return pubsub.PUnsubscribe(context.Background(), dropPatterns...)
	}

	dropTopics := r.router.Unsubscribe(sub.Upstream, sub.Topics...)
	if len(dropTopics) == 0 {
		return nil
	}

	return pubsub.Unsubscribe(context.Background(), dropTopics...)
}

// Stop closes the consumption and disconnects broker.
func (r *Redis) Stop() {
	if atomic.CompareAndSwapInt32(&r.stopped, 0, 1) {
		close(r.stop)
	}
}

// Subscribe broker to one or multiple channels.
func (r *Redis) Subscribe(upstream chan *broadcast.Message, topics ...string) error {
	if atomic.LoadInt32(&r.stopped) == 1 {
		return errors.New("broker has been stopped")
	}

	ctx := broadcast.Subscriber{Upstream: upstream, Topics: topics, Done: make(chan error)}

	r.listen <- ctx
	return <-ctx.Done
}

// SubscribePattern broker to pattern.
func (r *Redis) SubscribePattern(upstream chan *broadcast.Message, pattern string) error {
	if atomic.LoadInt32(&r.stopped) == 1 {
		return errors.New("broker has been stopped")
	}

	ctx := broadcast.Subscriber{Upstream: upstream, Pattern: pattern, Done: make(chan error)}

	r.listen <- ctx
	return <-ctx.Done
}

// Unsubscribe broker from one or multiple channels.
func (r *Redis) Unsubscribe(upstream chan *broadcast.Message, topics ...string) error {
	if atomic.LoadInt32(&r.stopped) == 1 {
		return errors.New("broker has been stopped")
	}

	ctx := broadcast.Subscriber{Upstream: upstream, Topics: topics, Done: make(chan error)}

	r.leave <- ctx
	return <-ctx.Done
}

// UnsubscribePattern broker from pattern.
func (r *Redis) UnsubscribePattern(upstream chan *broadcast.Message, pattern string) error {
	if atomic.LoadInt32(&r.stopped) == 1 {
		return errors.New("broker has been stopped")
	}

	ctx := broadcast.Subscriber{Upstream: upstream, Pattern: pattern, Done: make(chan error)}

	r.leave <- ctx
	return <-ctx.Done
}

// Publish one or multiple Channel.
func (r *Redis) Publish(messages ...*broadcast.Message) error {
	if atomic.LoadInt32(&r.stopped) == 1 {
		return errors.New("broker has been stopped")
	}

	for _, msg := range messages {
		if err := r.client.Publish(context.Background(), msg.Topic, []byte(msg.Payload)).Err(); err != nil {
			return err
		}
	}

	return nil
}
