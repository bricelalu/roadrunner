package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/spiral/roadrunner/service"
	"github.com/spiral/roadrunner/service/rpc"
	"github.com/spiral/roadrunner/v2/plugins/broadcast"
	"github.com/stretchr/testify/assert"
)

var rpcPort = 6010

func setup(cfg string) (*broadcast.Service, *rpc.Service, service.Container) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	c := service.NewContainer(logger)
	c.Register(rpc.ID, &rpc.Service{})
	c.Register(broadcast.ID, &broadcast.Service{})

	err := c.Init(&testCfg{
		broadcast: cfg,
		rpc:       fmt.Sprintf(`{"listen":"tcp://:%v"}`, rpcPort),
	})

	rpcPort++

	if err != nil {
		panic(err)
	}

	go func() {
		err = c.Serve()
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Millisecond * 100)

	b, _ := c.Get(broadcast.ID)
	br := b.(*broadcast.Service)

	r, _ := c.Get(rpc.ID)
	rp := r.(*rpc.Service)

	return br, rp, c
}

func readStr(m *broadcast.Message) string {
	return strings.TrimRight(string(m.Payload), "\n")
}

func newMessage(t, m string) *broadcast.Message {
	return &broadcast.Message{Topic: t, Payload: []byte(m)}
}

func TestService_Publish(t *testing.T) {
	svc := &broadcast.Service{}
	assert.Error(t, svc.Publish(nil))
}
