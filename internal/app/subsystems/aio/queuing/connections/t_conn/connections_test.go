package t_conn

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/metadata"
)

var (
	mockExecuteError = errors.New("execute error")
)

type (
	mockConnection struct {
		initCalled         bool
		taskCh             <-chan *ConnectionSubmission
		executeCalled      bool
		expectExecuteError bool
		executeError       error
	}
)

func (m *mockConnection) Init(tasks <-chan *ConnectionSubmission, cfg *ConnectionConfig) error {
	m.initCalled = true
	m.taskCh = tasks
	return nil
}

func (m *mockConnection) Task() <-chan *ConnectionSubmission {
	return m.taskCh
}

func (m *mockConnection) Execute(sub *ConnectionSubmission) error {
	m.executeCalled = true

	if m.expectExecuteError {
		m.executeError = mockExecuteError
		return m.executeError
	}

	return nil
}

func (m *mockConnection) String() string {
	return "mock"
}

func TestConnectionEventLoop(t *testing.T) {
	testCases := []struct {
		name               string
		connection         *mockConnection
		tasks              []*ConnectionSubmission
		expectedCalls      int
		expectExecuteError bool
		expectedError      error
	}{
		{
			name:       "Success",
			connection: &mockConnection{},
			tasks: []*ConnectionSubmission{
				{TaskId: "task1"},
				{TaskId: "task2"},
			},
			expectedCalls: 2,
			expectedError: nil,
		},
		{
			name:       "ExecuteError",
			connection: &mockConnection{expectExecuteError: true, executeError: errors.New("execute error")},
			tasks: []*ConnectionSubmission{
				{TaskId: "task1"},
			},
			expectedCalls:      1,
			expectExecuteError: true,
			expectedError:      mockExecuteError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			taskCh := make(chan *ConnectionSubmission, len(tc.tasks))
			for _, task := range tc.tasks {
				taskCh <- task
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel() // simulate a cancelled context from queuing subsystem.

			err := tc.connection.Init(taskCh, &ConnectionConfig{
				Kind:     HTTP,
				Name:     "test",
				Metadata: &metadata.Metadata{},
			})
			if err != nil {
				t.Fatalf("Failed to init connection: %v", err)
			}

			Start(ctx, tc.connection)

			if !tc.connection.initCalled {
				t.Error("Expected Init to be called")
			}

			if tc.connection.executeCalled != (tc.expectedCalls > 0) {
				t.Errorf("Expected Execute to be called: %v, but got: %v", tc.expectedCalls > 0, tc.connection.executeCalled)
			}

			if tc.expectedError != nil && !errors.Is(tc.connection.executeError, tc.expectedError) {
				t.Errorf("Expected execute error: %v, but got: %v", tc.expectedError, tc.connection.executeError)
			}
		})
	}
}
