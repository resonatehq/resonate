package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"
)

type StoreSubmission struct {
	Transaction *Transaction
}

func (s *StoreSubmission) String() string {
	return fmt.Sprintf("Store(transaction=Transaction(commands=%s))", s.Transaction.Commands)
}

type Transaction struct {
	Fence    *task.FencingToken
	Commands []Command
}

type Command interface {
	String() string
	isCommand()
}

type ReadPromiseCommand struct {
	Id string
}

func (c *ReadPromiseCommand) String() string {
	return "ReadPromise"
}

type ReadPromisesCommand struct {
	Time  int64
	Limit int
}

func (c *ReadPromisesCommand) String() string {
	return "ReadPromises"
}

type SearchPromisesCommand struct {
	Id     string
	States []promise.State
	Tags   map[string]string
	Limit  int
	SortId *int64
}

func (c *SearchPromisesCommand) String() string {
	return "SearchPromises"
}

type CreatePromiseCommand struct {
	Id             string
	Param          promise.Value
	Timeout        int64
	IdempotencyKey *idempotency.Key
	Tags           map[string]string
	CreatedOn      int64
}

func (c *CreatePromiseCommand) String() string {
	return "CreatePromise"
}

type UpdatePromiseCommand struct {
	Id             string
	State          promise.State
	Value          promise.Value
	IdempotencyKey *idempotency.Key
	CompletedOn    int64
}

func (c *UpdatePromiseCommand) String() string {
	return "UpdatePromise"
}

type CreateCallbackCommand struct {
	Id        string
	PromiseId string
	Recv      []byte
	Mesg      *message.Mesg
	Timeout   int64
	CreatedOn int64
}

func (c *CreateCallbackCommand) String() string {
	return "CreateCallback"
}

type DeleteCallbacksCommand struct {
	PromiseId string
}

func (c *DeleteCallbacksCommand) String() string {
	return "DeleteCallbacks"
}

type ReadScheduleCommand struct {
	Id string
}

func (c *ReadScheduleCommand) String() string {
	return "ReadSchedule"
}

type ReadSchedulesCommand struct {
	NextRunTime int64
	Limit       int
}

func (c *ReadSchedulesCommand) String() string {
	return "ReadSchedules"
}

type SearchSchedulesCommand struct {
	Id     string
	Tags   map[string]string
	Limit  int
	SortId *int64
}

func (c *SearchSchedulesCommand) String() string {
	return "SearchSchedules"
}

type CreateScheduleCommand struct {
	Id             string
	Description    string
	Cron           string
	Tags           map[string]string
	PromiseId      string
	PromiseTimeout int64
	PromiseParam   promise.Value
	PromiseTags    map[string]string
	NextRunTime    int64
	IdempotencyKey *idempotency.Key
	CreatedOn      int64
}

func (c *CreateScheduleCommand) String() string {
	return "CreateSchedule"
}

type UpdateScheduleCommand struct {
	Id          string
	LastRunTime *int64
	NextRunTime int64
}

func (c *UpdateScheduleCommand) String() string {
	return "UpdateSchedule"
}

type DeleteScheduleCommand struct {
	Id string
}

func (c *DeleteScheduleCommand) String() string {
	return "DeleteSchedule"
}

type ReadTaskCommand struct {
	Id string
}

func (c *ReadTaskCommand) String() string {
	return "ReadTask"
}

type ReadTasksCommand struct {
	States []task.State
	Time   int64
	Limit  int
}

func (c *ReadTasksCommand) String() string {
	return "ReadTasks"
}

type ReadEnqueueableTasksCommand struct {
	Time  int64
	Limit int
}

func (c *ReadEnqueueableTasksCommand) String() string {
	return "ReadEnqueueableTasks"
}

type CreateTaskCommand struct {
	Id        string
	Recv      []byte
	Mesg      *message.Mesg
	Timeout   int64
	ProcessId *string
	State     task.State
	Ttl       int64
	ExpiresAt int64
	CreatedOn int64
}

func (c *CreateTaskCommand) String() string {
	return "CreateTask"
}

type CreateTasksCommand struct {
	PromiseId string
	CreatedOn int64
}

func (c *CreateTasksCommand) String() string {
	return "CreateTasks"
}

type CompleteTasksCommand struct {
	RootPromiseId string
	CompletedOn   int64
}

func (c *CompleteTasksCommand) String() string {
	return "CompleteTasks"
}

type UpdateTaskCommand struct {
	Id             string
	ProcessId      *string
	State          task.State
	Counter        int
	Attempt        int
	Ttl            int64
	ExpiresAt      int64
	CompletedOn    *int64
	CurrentStates  []task.State
	CurrentCounter int
}

func (c *UpdateTaskCommand) String() string {
	return "UpdateTask"
}

type HeartbeatTasksCommand struct {
	ProcessId string
	Time      int64
}

func (c *HeartbeatTasksCommand) String() string {
	return "HeartbeatTasks"
}

type CreatePromiseAndTaskCommand struct {
	PromiseCommand *CreatePromiseCommand
	TaskCommand    *CreateTaskCommand
}

func (c *CreatePromiseAndTaskCommand) String() string {
	return "CreatePromiseAndTask"
}

type ReadLockCommand struct {
	ResourceId string
}

func (c *ReadLockCommand) String() string {
	return "ReadLock"
}

type AcquireLockCommand struct {
	ResourceId  string
	ProcessId   string
	ExecutionId string
	Ttl         int64
	ExpiresAt   int64
}

func (c *AcquireLockCommand) String() string {
	return "AcquireLock"
}

type ReleaseLockCommand struct {
	ResourceId  string
	ExecutionId string
}

func (c *ReleaseLockCommand) String() string {
	return "ReleaseLock"
}

type HeartbeatLocksCommand struct {
	ProcessId string
	Time      int64
}

func (c *HeartbeatLocksCommand) String() string {
	return "HeartbeatLocks"
}

type TimeoutLocksCommand struct {
	Timeout int64
}

func (c *TimeoutLocksCommand) String() string {
	return "TimeoutLocks"
}

func (*ReadPromiseCommand) isCommand()          {}
func (*ReadPromisesCommand) isCommand()         {}
func (*SearchPromisesCommand) isCommand()       {}
func (*CreatePromiseCommand) isCommand()        {}
func (*UpdatePromiseCommand) isCommand()        {}
func (*CreateCallbackCommand) isCommand()       {}
func (*DeleteCallbacksCommand) isCommand()      {}
func (*ReadScheduleCommand) isCommand()         {}
func (*ReadSchedulesCommand) isCommand()        {}
func (*SearchSchedulesCommand) isCommand()      {}
func (*CreateScheduleCommand) isCommand()       {}
func (*UpdateScheduleCommand) isCommand()       {}
func (*DeleteScheduleCommand) isCommand()       {}
func (*ReadTaskCommand) isCommand()             {}
func (*ReadTasksCommand) isCommand()            {}
func (*ReadEnqueueableTasksCommand) isCommand() {}
func (*CreateTaskCommand) isCommand()           {}
func (*CreateTasksCommand) isCommand()          {}
func (*CompleteTasksCommand) isCommand()        {}
func (*UpdateTaskCommand) isCommand()           {}
func (*HeartbeatTasksCommand) isCommand()       {}
func (*CreatePromiseAndTaskCommand) isCommand() {}
func (*ReadLockCommand) isCommand()             {}
func (*AcquireLockCommand) isCommand()          {}
func (*ReleaseLockCommand) isCommand()          {}
func (*HeartbeatLocksCommand) isCommand()       {}
func (*TimeoutLocksCommand) isCommand()         {}

type StoreCompletion struct {
	Valid   bool
	Results []Result
}

func (c *StoreCompletion) String() string {
    return fmt.Sprintf("Store(valid=%t, results=%s)", c.Valid, c.Results)
}

type Result interface {
	String() string
	isResult()
}

type QueryPromisesResult struct {
	RowsReturned int64
	LastSortId   int64
	Records      []*promise.PromiseRecord
}

func (r *QueryPromisesResult) String() string {
	return "QueryPromises"
}

type AlterPromisesResult struct {
	RowsAffected int64
}

func (r *AlterPromisesResult) String() string {
	return "AlterPromises"
}

type AlterCallbacksResult struct {
	RowsAffected int64
}

func (r *AlterCallbacksResult) String() string {
	return "AlterCallbacks"
}

type QuerySchedulesResult struct {
	RowsReturned int64
	LastSortId   int64
	Records      []*schedule.ScheduleRecord
}

func (r *QuerySchedulesResult) String() string {
	return "QuerySchedules"
}

type AlterSchedulesResult struct {
	RowsAffected int64
}

func (r *AlterSchedulesResult) String() string {
	return "AlterSchedules"
}

type QueryTasksResult struct {
	RowsReturned int64
	Records      []*task.TaskRecord
}

func (r *QueryTasksResult) String() string {
	return "QueryTasks"
}

type AlterTasksResult struct {
	RowsAffected int64
}

func (r *AlterTasksResult) String() string {
	return "AlterTasks"
}

type QueryLocksResult struct {
	RowsReturned int64
	Records      []*lock.LockRecord
}

func (r *QueryLocksResult) String() string {
	return "QueryLocks"
}

type AlterLocksResult struct {
	RowsAffected int64
}

func (r *AlterLocksResult) String() string {
	return "AlterLocks"
}

func (r *QueryPromisesResult) isResult()  {}
func (r *AlterPromisesResult) isResult()  {}
func (r *AlterCallbacksResult) isResult() {}
func (r *QuerySchedulesResult) isResult() {}
func (r *AlterSchedulesResult) isResult() {}
func (r *QueryTasksResult) isResult()     {}
func (r *AlterTasksResult) isResult()     {}

func (r *QueryLocksResult) isResult() {}
func (r *AlterLocksResult) isResult() {}

func AsQueryPromises(r Result) *QueryPromisesResult {
	return r.(*QueryPromisesResult)
}
func AsAlterPromises(r Result) *AlterPromisesResult {
	return r.(*AlterPromisesResult)
}
func AsAlterCallbacks(r Result) *AlterCallbacksResult {
	return r.(*AlterCallbacksResult)
}
func AsQuerySchedules(r Result) *QuerySchedulesResult {
	return r.(*QuerySchedulesResult)
}
func AsAlterSchedules(r Result) *AlterSchedulesResult {
	return r.(*AlterSchedulesResult)
}
func AsQueryTasks(r Result) *QueryTasksResult {
	return r.(*QueryTasksResult)
}
func AsAlterTasks(r Result) *AlterTasksResult {
	return r.(*AlterTasksResult)
}

func AsQueryLocks(r Result) *QueryLocksResult {
	return r.(*QueryLocksResult)
}
func AsAlterLocks(r Result) *AlterLocksResult {
	return r.(*AlterLocksResult)
}
