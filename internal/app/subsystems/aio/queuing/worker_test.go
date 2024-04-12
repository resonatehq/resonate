package queuing

import (
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

func TestQueuingWorker_Process(t *testing.T) {
	testCases := []struct {
		name        string
		submissions []*bus.SQE[t_aio.Submission, t_aio.Completion]
		completions []*bus.CQE[t_aio.Submission, t_aio.Completion]
	}{
		{
			name: "valid submissions",
			submissions: []*bus.SQE[t_aio.Submission, t_aio.Completion]{
				{
					Submission: &t_aio.Submission{
						Queuing: &t_aio.QueuingSubmission{
							TaskId:  "/test/task1",
							Counter: 1,
						},
					},
				},
				{
					Submission: &t_aio.Submission{
						Queuing: &t_aio.QueuingSubmission{
							TaskId:  "/test/task2",
							Counter: 2,
						},
					},
				},
			},
			completions: []*bus.CQE[t_aio.Submission, t_aio.Completion]{
				{
					Completion: &t_aio.Completion{
						Queuing: &t_aio.QueuingCompletion{
							Result: t_aio.Success,
						},
					},
				},
				{
					Completion: &t_aio.Completion{
						Queuing: &t_aio.QueuingCompletion{
							Result: t_aio.Success,
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			router := NewRouter()
			router.Handle("/test/*", &RouteHandler{
				Connection: "test_connection",
				Queue:      "test_queue",
			})

			connectionsSQ := make(map[string]chan *t_conn.ConnectionSubmission)
			connectionsSQ["test_connection"] = make(chan *t_conn.ConnectionSubmission, 10)

			worker := &QueuingWorker{
				BaseURL:          "http://localhost:8080",
				ConnectionRouter: router,
				ConnectionsSQ:    connectionsSQ,
			}

			cqes := worker.Process(tc.submissions)

			if len(tc.completions) != len(tc.submissions) {
				t.Errorf("expected completions length: %d, got: %d", len(tc.submissions), len(cqes))
			}

			for _, cqe := range cqes {
				if cqe.Completion.Queuing.Result != t_aio.Success {
					t.Errorf("expected success, got: %v", cqe.Completion.Queuing.Result)
				}
			}
		})
	}
}

func TestQueuingWorker_ProcessInvalidTaskId(t *testing.T) {
	router := NewRouter()

	worker := &QueuingWorker{
		BaseURL:          "http://localhost:8080",
		ConnectionRouter: router,
		ConnectionsSQ:    make(map[string]chan *t_conn.ConnectionSubmission),
	}

	sqes := []*bus.SQE[t_aio.Submission, t_aio.Completion]{
		{
			Submission: &t_aio.Submission{
				Queuing: &t_aio.QueuingSubmission{
					TaskId:  "invalid_task",
					Counter: 1,
				},
			},
		},
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic, got nil")
		}
	}()

	worker.Process(sqes)
}
