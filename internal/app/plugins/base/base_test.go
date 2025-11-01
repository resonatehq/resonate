package base

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type MockProcessor struct {
	process func(addr []byte, head map[string]string, body []byte) (bool, error)
	count   int
}

func (m *MockProcessor) Process(addr []byte, head map[string]string, body []byte) (bool, error) {
	m.count++
	if m.process != nil {
		return m.process(addr, head, body)
	}
	return true, nil
}

func TestNewPlugin(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
	config := &BaseConfig{
		Size:        10,
		Workers:     3,
		TimeToRetry: 15 * time.Second,
		TimeToClaim: 60 * time.Second,
	}

	proc := &MockProcessor{}
	plugin := NewPlugin(nil, "test", config, metrics, proc, nil)

	assert.NotNil(t, plugin)
	assert.Equal(t, "test", plugin.name)
	assert.Equal(t, 3, len(plugin.workers))
	assert.NotNil(t, plugin.sq)
}

func TestPluginString(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
	config := &BaseConfig{Size: 1, Workers: 1}
	proc := &MockProcessor{}

	plugin := NewPlugin(nil, "myplugin", config, metrics, proc, nil)
	assert.Contains(t, plugin.String(), "myplugin")
}

func TestPluginType(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
	config := &BaseConfig{Size: 1, Workers: 1}
	proc := &MockProcessor{}

	plugin := NewPlugin(nil, "mytype", config, metrics, proc, nil)
	assert.Equal(t, "mytype", plugin.Type())
}

func TestPluginEnqueue(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
	config := &BaseConfig{Size: 2, Workers: 1}
	proc := &MockProcessor{}

	plugin := NewPlugin(nil, "test", config, metrics, proc, nil)

	t.Run("SuccessfulEnqueue", func(t *testing.T) {
		msg := &aio.Message{
			Addr: []byte("addr"),
			Body: []byte("body"),
			Done: func(c *t_aio.SenderCompletion) {},
		}
		assert.True(t, plugin.Enqueue(msg))
	})

	t.Run("NilMessage", func(t *testing.T) {
		assert.False(t, plugin.Enqueue(nil))
	})

	t.Run("QueueFull", func(t *testing.T) {
		new := NewPlugin(nil, "test", &BaseConfig{Size: 1, Workers: 1}, metrics, proc, nil)

		msg1 := &aio.Message{Addr: []byte("1"), Body: []byte("1"), Done: func(c *t_aio.SenderCompletion) {}}
		msg2 := &aio.Message{Addr: []byte("2"), Body: []byte("2"), Done: func(c *t_aio.SenderCompletion) {}}

		assert.True(t, new.Enqueue(msg1))
		assert.False(t, new.Enqueue(msg2))
	})
}

func TestPluginStartStop(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
	config := &BaseConfig{Size: 10, Workers: 2}
	proc := &MockProcessor{}

	plugin := NewPlugin(nil, "test", config, metrics, proc, nil)

	t.Run("Start", func(t *testing.T) {
		err := plugin.Start(nil)
		assert.Nil(t, err)
	})

	t.Run("Stop", func(t *testing.T) {
		err := plugin.Stop()
		assert.Nil(t, err)
	})
}

func TestPluginStopWithCleanup(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
	config := &BaseConfig{Size: 1, Workers: 1}
	proc := &MockProcessor{}

	is_cleanup := false
	cleanup := func() error {
		is_cleanup = true
		return nil
	}

	plugin := NewPlugin(nil, "test", config, metrics, proc, cleanup)
	err := plugin.Stop()

	assert.Nil(t, err)
	assert.True(t, is_cleanup)
}

func TestPluginStopWithCleanupError(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
	config := &BaseConfig{Size: 1, Workers: 1}
	proc := &MockProcessor{}

	expected_err := errors.New("cleanup failed")
	cleanup := func() error {
		return expected_err
	}

	plugin := NewPlugin(nil, "test", config, metrics, proc, cleanup)
	err := plugin.Stop()

	assert.Equal(t, expected_err, err)
}

func TestWorkerProcessing(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
	config := &BaseConfig{
		Size:        10,
		Workers:     1,
		TimeToRetry: 10 * time.Second,
		TimeToClaim: 30 * time.Second,
	}

	t.Run("SuccessfulProcessing", func(t *testing.T) {
		proc := &MockProcessor{
			process: func(addr []byte, head map[string]string, body []byte) (bool, error) {
				assert.Equal(t, []byte("test addr"), addr)
				assert.Equal(t, []byte("test body"), body)
				assert.Equal(t, map[string]string{"foo": "bar"}, head)
				return true, nil
			},
		}

		plugin := NewPlugin(nil, "test", config, metrics, proc, nil)
		assert.Nil(t, plugin.Start(nil))
		defer func() {
			assert.Nil(t, plugin.Stop())
		}()

		done := make(chan bool, 1)
		msg := &aio.Message{
			Addr: []byte("test addr"),
			Head: map[string]string{"foo": "bar"},
			Body: []byte("test body"),
			Done: func(c *t_aio.SenderCompletion) {
				assert.True(t, c.Success)
				assert.Equal(t, int64(10000), c.TimeToRetry)
				assert.Equal(t, int64(30000), c.TimeToClaim)
				done <- true
			},
		}

		assert.True(t, plugin.Enqueue(msg))

		select {
		case <-done:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for processing")
		}

		assert.Equal(t, 1, proc.count)
	})

	t.Run("FailedProcessing", func(t *testing.T) {
		proc := &MockProcessor{
			process: func(addr []byte, head map[string]string, body []byte) (bool, error) {
				return false, errors.New("processing failed")
			},
		}

		plugin := NewPlugin(nil, "test", config, metrics, proc, nil)
		assert.Nil(t, plugin.Start(nil))
		defer func() {
			assert.Nil(t, plugin.Stop())
		}()

		done := make(chan bool, 1)
		msg := &aio.Message{
			Addr: []byte("addr"),
			Body: []byte("body"),
			Done: func(c *t_aio.SenderCompletion) {
				assert.False(t, c.Success)
				done <- true
			},
		}

		assert.True(t, plugin.Enqueue(msg))

		select {
		case <-done:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for processing")
		}
	})
}

func TestWorkerString(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
	config := &BaseConfig{Size: 1, Workers: 1}
	proc := &MockProcessor{}

	plugin := NewPlugin(nil, "testplugin", config, metrics, proc, nil)
	assert.Contains(t, plugin.workers[0].String(), "testplugin")
}

func TestMultipleWorkers(t *testing.T) {
	metrics := metrics.New(prometheus.NewRegistry())
	config := &BaseConfig{
		Size:    10,
		Workers: 3,
	}

	proc := &MockProcessor{}
	plugin := NewPlugin(nil, "test", config, metrics, proc, nil)

	assert.Equal(t, 3, len(plugin.workers))
	for i, worker := range plugin.workers {
		assert.Equal(t, i, worker.id)
		assert.Equal(t, proc, worker.processor)
		assert.Equal(t, config, worker.config)
	}
}

func TestBaseConfig(t *testing.T) {
	config := BaseConfig{
		Size:        100,
		Workers:     5,
		TimeToRetry: 15 * time.Second,
		TimeToClaim: 60 * time.Second,
	}

	assert.Equal(t, 100, config.Size)
	assert.Equal(t, 5, config.Workers)
	assert.Equal(t, int64(15000), config.TimeToRetry.Milliseconds())
	assert.Equal(t, int64(60000), config.TimeToClaim.Milliseconds())
}
