package tests

import (
	"testing"

	"github.com/spiral/roadrunner/v2/plugins/broadcast/websockets"
	"github.com/stretchr/testify/assert"
)

func TestConnContext_ManageTopics(t *testing.T) {
	ctx := &ws.ConnContext{Topics: make([]string, 0)}

	assert.Equal(t, []string{}, ctx.Topics)

	ctx.addTopics("a", "b")
	assert.Equal(t, []string{"a", "b"}, ctx.Topics)

	ctx.addTopics("a", "c")
	assert.Equal(t, []string{"a", "b", "c"}, ctx.Topics)

	ctx.dropTopic("b", "c")
	assert.Equal(t, []string{"a"}, ctx.Topics)

	ctx.dropTopic("b", "c")
	assert.Equal(t, []string{"a"}, ctx.Topics)

	ctx.dropTopic("a")
	assert.Equal(t, []string{}, ctx.Topics)
}
