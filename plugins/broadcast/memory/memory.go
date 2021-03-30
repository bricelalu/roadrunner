package memory

import (
	"errors"
	"sync/atomic"

	"github.com/spiral/roadrunner/v2/plugins/broadcast"
)

// Memory manages broadcasting in memory.
type Memory struct {
	router      *broadcast.Router
	messages    chan *broadcast.Message
	join, leave chan broadcast.subscriber
	stop        chan interface{}
	stopped     int32
}

// memoryBroker creates new memory based message broker.
func memoryBroker() *Memory {
	return &Memory{
		router:   broadcast.NewRouter(),
		messages: make(chan *broadcast.Message),
		join:     make(chan broadcast.subscriber),
		leave:    make(chan broadcast.subscriber),
		stop:     make(chan interface{}),
		stopped:  0,
	}
}

// Serve serves broker.
func (m *Memory) Serve() error {
	for {
		select {
		case ctx := <-m.join:
			ctx.done <- m.handleJoin(ctx)
		case ctx := <-m.leave:
			ctx.done <- m.handleLeave(ctx)
		case msg := <-m.messages:
			m.router.Dispatch(msg)
		case <-m.stop:
			return nil
		}
	}
}

func (m *Memory) handleJoin(sub broadcast.subscriber) (err error) {
	if sub.pattern != "" {
		_, err = m.router.SubscribePattern(sub.upstream, sub.pattern)
		return err
	}

	m.router.Subscribe(sub.upstream, sub.topics...)
	return nil
}

func (m *Memory) handleLeave(sub broadcast.subscriber) error {
	if sub.pattern != "" {
		m.router.UnsubscribePattern(sub.upstream, sub.pattern)
		return nil
	}

	m.router.Unsubscribe(sub.upstream, sub.topics...)
	return nil
}

// Stop closes the consumption and disconnects broker.
func (m *Memory) Stop() {
	if atomic.CompareAndSwapInt32(&m.stopped, 0, 1) {
		close(m.stop)
	}
}

// Subscribe broker to one or multiple channels.
func (m *Memory) Subscribe(upstream chan *broadcast.Message, topics ...string) error {
	if atomic.LoadInt32(&m.stopped) == 1 {
		return errors.New("broker has been stopped")
	}

	ctx := broadcast.subscriber{upstream: upstream, topics: topics, done: make(chan error)}

	m.join <- ctx
	return <-ctx.done
}

// SubscribePattern broker to pattern.
func (m *Memory) SubscribePattern(upstream chan *broadcast.Message, pattern string) error {
	if atomic.LoadInt32(&m.stopped) == 1 {
		return errors.New("broker has been stopped")
	}

	ctx := broadcast.subscriber{upstream: upstream, pattern: pattern, done: make(chan error)}

	m.join <- ctx
	return <-ctx.done
}

// Unsubscribe broker from one or multiple channels.
func (m *Memory) Unsubscribe(upstream chan *broadcast.Message, topics ...string) error {
	if atomic.LoadInt32(&m.stopped) == 1 {
		return errors.New("broker has been stopped")
	}

	ctx := broadcast.subscriber{upstream: upstream, topics: topics, done: make(chan error)}

	m.leave <- ctx
	return <-ctx.done
}

// UnsubscribePattern broker from pattern.
func (m *Memory) UnsubscribePattern(upstream chan *broadcast.Message, pattern string) error {
	if atomic.LoadInt32(&m.stopped) == 1 {
		return errors.New("broker has been stopped")
	}

	ctx := broadcast.subscriber{upstream: upstream, pattern: pattern, done: make(chan error)}

	m.leave <- ctx
	return <-ctx.done
}

// Publish one or multiple Channel.
func (m *Memory) Publish(messages ...*broadcast.Message) error {
	if atomic.LoadInt32(&m.stopped) == 1 {
		return errors.New("broker has been stopped")
	}

	for _, msg := range messages {
		m.messages <- msg
	}

	return nil
}
