package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"

	_ "github.com/lib/pq"
)

const (
	CREATE_TABLE_STATEMENT = `
	CREATE TABLE IF NOT EXISTS promises (
		id                           TEXT,
		sort_id                      SERIAL,
		state                        INTEGER DEFAULT 1,
		param_headers                JSONB,
		param_data                   BYTEA,
		value_headers                JSONB,
		value_data                   BYTEA,
		timeout                      BIGINT,
		idempotency_key_for_create   TEXT,
		idempotency_key_for_complete TEXT,
		tags                         JSONB,
		created_on                   BIGINT,
		completed_on                 BIGINT,
		PRIMARY KEY(id)
	);

	CREATE INDEX IF NOT EXISTS idx_promises_sort_id ON promises(sort_id);

	CREATE TABLE IF NOT EXISTS callbacks (
		id              TEXT UNIQUE,
		promise_id      TEXT,
		root_promise_id TEXT,
		recv            BYTEA,
		mesg            BYTEA,
		timeout         BIGINT,
		created_on      BIGINT
	);

	CREATE TABLE IF NOT EXISTS schedules (
		id                    TEXT,
		sort_id               SERIAL,
		description           TEXT,
		cron                  TEXT,
		tags                  JSONB,
		promise_id            TEXT,
		promise_timeout       BIGINT,
		promise_param_headers JSONB,
		promise_param_data    BYTEA,
		promise_tags          JSONB,
		last_run_time         BIGINT,
		next_run_time         BIGINT,
		idempotency_key       TEXT,
		created_on            BIGINT,
		PRIMARY KEY(id)
	);

	CREATE INDEX IF NOT EXISTS idx_schedules_sort_id ON schedules(sort_id);
	CREATE INDEX IF NOT EXISTS idx_schedules_next_run_time ON schedules(next_run_time);

	CREATE TABLE IF NOT EXISTS locks (
		resource_id  TEXT,
		execution_id TEXT,
		process_id   TEXT,
		ttl          BIGINT,
		expires_at   BIGINT,
		PRIMARY KEY(resource_id)
	);

	CREATE INDEX IF NOT EXISTS idx_locks_id ON locks(resource_id, execution_id);
	CREATE INDEX IF NOT EXISTS idx_locks_process_id ON locks(process_id);
	CREATE INDEX IF NOT EXISTS idx_locks_expires_at ON locks(expires_at);

	CREATE TABLE IF NOT EXISTS tasks (
		id              TEXT,
		sort_id         SERIAL,
		process_id      TEXT,
		state           INTEGER DEFAULT 1,
		root_promise_id TEXT,
		recv            BYTEA,
		mesg            BYTEA,
		timeout         BIGINT,
		counter         INTEGER DEFAULT 1,
		attempt         INTEGER DEFAULT 0,
		ttl             BIGINT DEFAULT 0,
		expires_at      BIGINT DEFAULT 0,
		created_on      BIGINT,
		completed_on    BIGINT,
		PRIMARY KEY(id)
	);

	CREATE INDEX IF NOT EXISTS idx_tasks_process_id ON tasks(process_id);
	CREATE INDEX IF NOT EXISTS idx_tasks_expires_at ON tasks(expires_at);

	CREATE TABLE IF NOT EXISTS migrations (
		id INTEGER,
		PRIMARY KEY(id)
	);

	INSERT INTO migrations (id) VALUES (1) ON CONFLICT(id) DO NOTHING;`

	DROP_TABLE_STATEMENT = `
	DROP TABLE promises;
	DROP TABLE callbacks;
	DROP TABLE schedules;
	DROP TABLE locks;
	DROP TABLE tasks;
	DROP TABLE migrations;`

	PROMISE_SELECT_STATEMENT = `
	SELECT
		id, state, param_headers, param_data, value_headers, value_data, timeout, idempotency_key_for_create, idempotency_key_for_complete, tags, created_on, completed_on
	FROM
		promises
	WHERE
		id = $1`

	PROMISE_SELECT_ALL_STATEMENT = `
	SELECT
		id, state, param_headers, param_data, value_headers, value_data, timeout, idempotency_key_for_create, idempotency_key_for_complete, tags, created_on, completed_on, sort_id
	FROM
		promises
	WHERE
		state = 1 AND timeout <= $1
	LIMIT
		$2`

	PROMISE_SEARCH_STATEMENT = `
	SELECT
		id, state, param_headers, param_data, value_headers, value_data, timeout, idempotency_key_for_create, idempotency_key_for_complete, tags, created_on, completed_on, sort_id
	FROM
		promises
	WHERE
		($1::int IS NULL OR sort_id < $1) AND
		id LIKE $2 AND
		state & $3 != 0 AND
		($4::jsonb IS NULL OR tags @> $4)
	ORDER BY
		sort_id DESC
	LIMIT
		$5`

	PROMISE_INSERT_STATEMENT = `
	INSERT INTO promises
		(id, param_headers, param_data, timeout, idempotency_key_for_create, tags, created_on)
	VALUES
		($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT(id) DO NOTHING`

	PROMISE_UPDATE_STATEMENT = `
	UPDATE
		promises
	SET
		state = $1, value_headers = $2, value_data = $3, idempotency_key_for_complete = $4, completed_on = $5
	WHERE
		id = $6 AND state = 1`

	CALLBACK_INSERT_STATEMENT = `
	INSERT INTO callbacks
		(id, promise_id, root_promise_id, recv, mesg, timeout, created_on)
	SELECT
		$1, $2, $3, $4, $5, $6, $7
	WHERE EXISTS
		(SELECT 1 FROM promises WHERE id = $2 AND state = 1)
	AND NOT EXISTS
		(SELECT 1 FROM callbacks WHERE id = $1)`

	CALLBACK_DELETE_STATEMENT = `
	DELETE FROM callbacks WHERE promise_id = $1`

	SCHEDULE_SELECT_STATEMENT = `
	SELECT
		id, description, cron, tags, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, last_run_time, next_run_time, idempotency_key, created_on
	FROM
		schedules
	WHERE
		id = $1`

	SCHEDULE_SELECT_ALL_STATEMENT = `
	SELECT
		id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, last_run_time, next_run_time
	FROM
		schedules
	WHERE
		next_run_time <= $1
	ORDER BY
		next_run_time ASC, sort_id ASC
	LIMIT
		$2`

	SCHEDULE_SEARCH_STATEMENT = `
	SELECT
		id, cron, tags, last_run_time, next_run_time, idempotency_key, created_on, sort_id
	FROM
		schedules
	WHERE
		($1::int IS NULL OR sort_id < $1) AND
		id LIKE $2 AND
		($3::jsonb IS NULL OR tags @> $3)
	ORDER BY
		sort_id DESC
	LIMIT
		$4`

	SCHEDULE_INSERT_STATEMENT = `
	INSERT INTO schedules
		(id, description, cron, tags, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, next_run_time, idempotency_key, created_on)
	VALUES
		($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	ON CONFLICT(id) DO NOTHING`

	SCHEDULE_UPDATE_STATEMENT = `
	UPDATE
		schedules
	SET
		last_run_time = next_run_time, next_run_time = $1
	WHERE
		id = $2 AND next_run_time = $3`

	SCHEDULE_DELETE_STATEMENT = `
	DELETE FROM schedules WHERE id = $1`

	LOCK_READ_STATEMENT = `
	SELECT
		resource_id, process_id, execution_id, ttl, expires_at
	FROM
		locks
	WHERE
		resource_id = $1`

	LOCK_ACQUIRE_STATEMENT = `
	INSERT INTO locks
		(resource_id, execution_id, process_id, ttl, expires_at)
	VALUES
		($1, $2, $3, $4, $5)
	ON CONFLICT(resource_id)
	DO UPDATE SET
		process_id = EXCLUDED.process_id,
		ttl = EXCLUDED.ttl,
		expires_at = EXCLUDED.expires_at
	WHERE locks.execution_id = EXCLUDED.execution_id`

	LOCK_RELEASE_STATEMENT = `
	DELETE FROM locks WHERE resource_id = $1 AND execution_id = $2`

	LOCK_HEARTBEAT_STATEMENT = `
	UPDATE
		locks
	SET
		expires_at = $1 + ttl
	WHERE
		process_id = $2`

	LOCK_TIMEOUT_STATEMENT = `
	DELETE FROM locks WHERE expires_at <= $1`

	TASK_SELECT_STATEMENT = `
	SELECT
		id, process_id, state, root_promise_id, recv, mesg, timeout, counter, attempt, ttl, expires_at, created_on, completed_on
	FROM
		tasks
	WHERE
		id = $1`

	TASK_SELECT_ALL_STATEMENT = `
	SELECT
		id,
		process_id,
		state,
		root_promise_id,
		recv,
		mesg,
		timeout,
		counter,
		attempt,
		ttl,
		expires_at,
		created_on,
		completed_on
	FROM tasks
	WHERE
		state & $1 != 0 AND (expires_at <= $2 OR timeout <= $2)
	ORDER BY root_promise_id, sort_id ASC
	LIMIT $3`

	TASK_SELECT_ENQUEUEABLE_STATEMENT = `
	SELECT DISTINCT ON (root_promise_id)
		id,
		process_id,
		state,
		root_promise_id,
		recv,
		mesg,
		timeout,
		counter,
		attempt,
		ttl,
		expires_at,
		created_on,
		completed_on
	FROM tasks t1
	WHERE
		state = 1
	AND NOT EXISTS (
		SELECT 1
		FROM tasks t2
		WHERE t2.root_promise_id = t1.root_promise_id
		AND t2.state in (2, 4) -- 2 -> Enqueue, 4 -> Claimed
	)
	ORDER BY root_promise_id, sort_id ASC
	LIMIT $1`

	TASK_INSERT_STATEMENT = `
	INSERT INTO tasks
		(id, recv, mesg, timeout, process_id, state, root_promise_id, ttl, expires_at, created_on)
	VALUES
		($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT(id) DO NOTHING`

	TASK_INSERT_ALL_STATEMENT = `
	INSERT INTO tasks
		(id, recv, mesg, timeout, root_promise_id, created_on)
	SELECT
		id, recv, mesg, timeout, root_promise_id, $1
	FROM
		callbacks
	WHERE
		promise_id = $2
	ORDER BY
		id`

	TASK_UPDATE_STATEMENT = `
	UPDATE
		tasks
	SET
		process_id = $1, state = $2, counter = $3, attempt = $4, ttl = $5, expires_at = $6, completed_on = $7
	WHERE
		id = $8 AND state & $9 != 0 AND counter = $10`

	TASK_COMPLETE_BY_ROOT_ID_STATEMENT = `
	UPDATE
		tasks
	SET
		state = 8, completed_on = $1 -- State = 8 -> Completed
	WHERE
		root_promise_id = $2 AND state in (1, 2, 4) -- State in (Init, Enqueued, Claimed)`

	TASK_HEARTBEAT_STATEMENT = `
	UPDATE
		tasks
	SET
		expires_at = $1 + ttl
	WHERE
		process_id = $2 AND state = 4`
)

// Config

type Config struct {
	Size      int               `flag:"size" desc:"submission buffered channel size" default:"1000"`
	BatchSize int               `flag:"batch-size" desc:"max submissions processed per iteration" default:"1000"`
	Workers   int               `flag:"workers" desc:"number of workers" default:"1" dst:"1"`
	Host      string            `flag:"host" desc:"postgres host" default:"localhost"`
	Port      string            `flag:"port" desc:"postgres port" default:"5432"`
	Username  string            `flag:"username" desc:"postgres username"`
	Password  string            `flag:"password" desc:"postgres password"`
	Database  string            `flag:"database" desc:"postgres database" default:"resonate" dst:"resonate_dst"`
	Query     map[string]string `flag:"query" desc:"postgres query options" dst:"{\"sslmode\":\"disable\"}"`
	TxTimeout time.Duration     `flag:"tx-timeout" desc:"postgres transaction timeout" default:"10s"`
	Reset     bool              `flag:"reset" desc:"reset postgres db on shutdown" default:"false" dst:"true"`
}

// Subsystem

type PostgresStore struct {
	config  *Config
	sq      chan<- *bus.SQE[t_aio.Submission, t_aio.Completion]
	db      *sql.DB
	workers []*PostgresStoreWorker
}

func New(aio aio.AIO, metrics *metrics.Metrics, config *Config) (*PostgresStore, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*PostgresStoreWorker, config.Workers)

	rawQuery := make([]string, len(config.Query))
	for i, q := range util.OrderedRangeKV(config.Query) {
		rawQuery[i] = fmt.Sprintf("%s=%s", q.Key, q.Value)
	}

	dbUrl := &url.URL{
		User:     url.UserPassword(config.Username, config.Password),
		Host:     fmt.Sprintf("%s:%s", config.Host, config.Port),
		Path:     config.Database,
		Scheme:   "postgres",
		RawQuery: strings.Join(rawQuery, "&"),
	}

	db, err := sql.Open("postgres", dbUrl.String())
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(config.Workers)
	db.SetMaxIdleConns(config.Workers)
	db.SetConnMaxIdleTime(0)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &PostgresStoreWorker{
			config:  config,
			i:       i,
			db:      db,
			sq:      sq,
			flush:   make(chan int64, 1),
			aio:     aio,
			metrics: metrics,
		}
	}

	return &PostgresStore{
		config:  config,
		sq:      sq,
		db:      db,
		workers: workers,
	}, nil
}

func (s *PostgresStore) String() string {
	return "store:postgres"
}

func (s *PostgresStore) Kind() t_aio.Kind {
	return t_aio.Store
}

func (s *PostgresStore) Start(chan<- error) error {
	if _, err := s.db.Exec(CREATE_TABLE_STATEMENT); err != nil {
		return err
	}

	for _, worker := range s.workers {
		go worker.Start()
	}

	return nil
}

func (s *PostgresStore) Stop() error {
	close(s.sq)

	if s.config.Reset {
		if err := s.Reset(); err != nil {
			return err
		}
	}

	return s.db.Close()
}

func (s *PostgresStore) Reset() error {
	if _, err := s.db.Exec(DROP_TABLE_STATEMENT); err != nil {
		return err
	}

	return nil
}

func (s *PostgresStore) Enqueue(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) bool {
	select {
	case s.sq <- sqe:
		return true
	default:
		return false
	}
}

func (s *PostgresStore) Flush(t int64) {
	for _, worker := range s.workers {
		worker.Flush(t)
	}
}

func (s *PostgresStore) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(len(s.workers) > 0, "must be at least one worker")
	return s.workers[0].Process(sqes)
}

// Worker

type PostgresStoreWorker struct {
	config  *Config
	i       int
	db      *sql.DB
	sq      <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	flush   chan int64
	aio     aio.AIO
	metrics *metrics.Metrics
}

func (w *PostgresStoreWorker) String() string {
	return "store:postgres"
}

func (w *PostgresStoreWorker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.i))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for {
		sqes, ok := store.Collect(w.sq, w.flush, w.config.BatchSize)
		if len(sqes) > 0 {
			counter.Set(float64(len(sqes)))
			for _, cqe := range w.Process(sqes) {
				w.aio.EnqueueCQE(cqe)
				counter.Dec()
			}
		}
		if !ok {
			return
		}
	}
}

func (w *PostgresStoreWorker) Flush(t int64) {
	// ignore case where flush channel is full,
	// this means the flush is waiting on the cq
	select {
	case w.flush <- t:
	default:
	}
}

func (w *PostgresStoreWorker) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	return store.Process(w, sqes)
}

func (w *PostgresStoreWorker) Execute(transactions []*t_aio.Transaction) ([][]t_aio.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), w.config.TxTimeout)
	defer cancel()

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	results, err := w.performCommands(tx, transactions)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			err = fmt.Errorf("tx failed: %v, unable to rollback: %v", err, rbErr)
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return results, nil
}

func (w *PostgresStoreWorker) performCommands(tx *sql.Tx, transactions []*t_aio.Transaction) ([][]t_aio.Result, error) {
	// Lazily defined prepared statements
	var promiseInsertStmt *sql.Stmt
	var promiseUpdateStmt *sql.Stmt
	var callbackInsertStmt *sql.Stmt
	var callbackDeleteStmt *sql.Stmt
	var scheduleInsertStmt *sql.Stmt
	var scheduleUpdateStmt *sql.Stmt
	var scheduleDeleteStmt *sql.Stmt
	var lockAcquireStmt *sql.Stmt
	var lockReleaseStmt *sql.Stmt
	var lockHeartbeatStmt *sql.Stmt
	var lockTimeoutStmt *sql.Stmt
	var tasksInsertStmt *sql.Stmt
	var taskInsertStmt *sql.Stmt
	var taskUpdateStmt *sql.Stmt
	var tasksCompleteStmt *sql.Stmt
	var taskHeartbeatStmt *sql.Stmt

	// Results
	results := make([][]t_aio.Result, len(transactions))

	for i, transaction := range transactions {
		util.Assert(len(transaction.Commands) > 0, "expected a command")
		results[i] = make([]t_aio.Result, len(transaction.Commands))

		for j, command := range transaction.Commands {
			var err error

			switch v := command.(type) {
			// Promises
			case *t_aio.ReadPromiseCommand:
				results[i][j], err = w.readPromise(tx, v)
			case *t_aio.ReadPromisesCommand:
				results[i][j], err = w.readPromises(tx, v)
			case *t_aio.SearchPromisesCommand:
				results[i][j], err = w.searchPromises(tx, v)
			case *t_aio.CreatePromiseCommand:
				if promiseInsertStmt == nil {
					promiseInsertStmt, err = tx.Prepare(PROMISE_INSERT_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.createPromise(tx, promiseInsertStmt, v)
			case *t_aio.UpdatePromiseCommand:
				if promiseUpdateStmt == nil {
					promiseUpdateStmt, err = tx.Prepare(PROMISE_UPDATE_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.updatePromise(tx, promiseUpdateStmt, v)

			// Callbacks
			case *t_aio.CreateCallbackCommand:
				if callbackInsertStmt == nil {
					callbackInsertStmt, err = tx.Prepare(CALLBACK_INSERT_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.createCallback(tx, callbackInsertStmt, v)
			case *t_aio.DeleteCallbacksCommand:
				if callbackDeleteStmt == nil {
					callbackDeleteStmt, err = tx.Prepare(CALLBACK_DELETE_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.deleteCallbacks(tx, callbackDeleteStmt, v)

			// Schedules
			case *t_aio.ReadScheduleCommand:
				results[i][j], err = w.readSchedule(tx, v)
			case *t_aio.ReadSchedulesCommand:
				results[i][j], err = w.readSchedules(tx, v)
			case *t_aio.SearchSchedulesCommand:
				results[i][j], err = w.searchSchedules(tx, v)
			case *t_aio.CreateScheduleCommand:
				if scheduleInsertStmt == nil {
					scheduleInsertStmt, err = tx.Prepare(SCHEDULE_INSERT_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.createSchedule(tx, scheduleInsertStmt, v)
			case *t_aio.UpdateScheduleCommand:
				if scheduleUpdateStmt == nil {
					scheduleUpdateStmt, err = tx.Prepare(SCHEDULE_UPDATE_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.updateSchedule(tx, scheduleUpdateStmt, v)
			case *t_aio.DeleteScheduleCommand:
				if scheduleDeleteStmt == nil {
					scheduleDeleteStmt, err = tx.Prepare(SCHEDULE_DELETE_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.deleteSchedule(tx, scheduleDeleteStmt, v)

			// Locks
			case *t_aio.ReadLockCommand:
				results[i][j], err = w.readLock(tx, v)
			case *t_aio.AcquireLockCommand:
				if lockAcquireStmt == nil {
					lockAcquireStmt, err = tx.Prepare(LOCK_ACQUIRE_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.acquireLock(tx, lockAcquireStmt, v)
			case *t_aio.ReleaseLockCommand:
				if lockReleaseStmt == nil {
					lockReleaseStmt, err = tx.Prepare(LOCK_RELEASE_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.releaseLock(tx, lockReleaseStmt, v)
			case *t_aio.HeartbeatLocksCommand:
				if lockHeartbeatStmt == nil {
					lockHeartbeatStmt, err = tx.Prepare(LOCK_HEARTBEAT_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.hearbeatLocks(tx, lockHeartbeatStmt, v)
			case *t_aio.TimeoutLocksCommand:
				if lockTimeoutStmt == nil {
					lockTimeoutStmt, err = tx.Prepare(LOCK_TIMEOUT_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.timeoutLocks(tx, lockTimeoutStmt, v)

			// Tasks
			case *t_aio.ReadTaskCommand:
				results[i][j], err = w.readTask(tx, v)
			case *t_aio.ReadTasksCommand:
				results[i][j], err = w.readTasks(tx, v)
			case *t_aio.ReadEnqueueableTasksCommand:
				results[i][j], err = w.readEnqueueableTasks(tx, v)
			case *t_aio.CreateTaskCommand:
				if taskInsertStmt == nil {
					taskInsertStmt, err = tx.Prepare(TASK_INSERT_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.createTask(tx, taskInsertStmt, v)
			case *t_aio.CreateTasksCommand:
				if tasksInsertStmt == nil {
					tasksInsertStmt, err = tx.Prepare(TASK_INSERT_ALL_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.createTasks(tx, tasksInsertStmt, v)
			case *t_aio.UpdateTaskCommand:
				if taskUpdateStmt == nil {
					taskUpdateStmt, err = tx.Prepare(TASK_UPDATE_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.updateTask(tx, taskUpdateStmt, v)
			case *t_aio.CompleteTasksCommand:
				if tasksCompleteStmt == nil {
					tasksCompleteStmt, err = tx.Prepare(TASK_COMPLETE_BY_ROOT_ID_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.completeTasks(tx, tasksCompleteStmt, v)
			case *t_aio.HeartbeatTasksCommand:
				if taskHeartbeatStmt == nil {
					taskHeartbeatStmt, err = tx.Prepare(TASK_HEARTBEAT_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.heartbeatTasks(tx, taskHeartbeatStmt, v)

			case *t_aio.CreatePromiseAndTaskCommand:
				if promiseInsertStmt == nil {
					promiseInsertStmt, err = tx.Prepare(PROMISE_INSERT_STATEMENT)
					if err != nil {
						return nil, err
					}
				}

				if taskInsertStmt == nil {
					taskInsertStmt, err = tx.Prepare(TASK_INSERT_STATEMENT)
					if err != nil {
						return nil, err
					}
				}
				results[i][j], err = w.createPromiseAndTask(tx, promiseInsertStmt, taskInsertStmt, v)

			default:
				panic(fmt.Sprintf("invalid command: %s", command.String()))
			}

			if err != nil {
				return nil, err
			}
		}
	}

	return results, nil
}

// Promises

func (w *PostgresStoreWorker) readPromise(tx *sql.Tx, cmd *t_aio.ReadPromiseCommand) (*t_aio.QueryPromisesResult, error) {
	// select
	row := tx.QueryRow(PROMISE_SELECT_STATEMENT, cmd.Id)
	record := &promise.PromiseRecord{}
	rowsReturned := int64(1)

	if err := row.Scan(
		&record.Id,
		&record.State,
		&record.ParamHeaders,
		&record.ParamData,
		&record.ValueHeaders,
		&record.ValueData,
		&record.Timeout,
		&record.IdempotencyKeyForCreate,
		&record.IdempotencyKeyForComplete,
		&record.Tags,
		&record.CreatedOn,
		&record.CompletedOn,
	); err != nil {
		if err == sql.ErrNoRows {
			rowsReturned = 0
		} else {
			return nil, err
		}
	}

	var records []*promise.PromiseRecord
	if rowsReturned == 1 {
		records = append(records, record)
	}

	return &t_aio.QueryPromisesResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *PostgresStoreWorker) readPromises(tx *sql.Tx, cmd *t_aio.ReadPromisesCommand) (*t_aio.QueryPromisesResult, error) {
	// select
	rows, err := tx.Query(PROMISE_SELECT_ALL_STATEMENT, cmd.Time, cmd.Limit)
	if err != nil {
		return nil, err
	}

	rowsReturned := int64(0)
	var records []*promise.PromiseRecord
	var lastSortId int64

	for rows.Next() {
		record := &promise.PromiseRecord{}
		if err := rows.Scan(
			&record.Id,
			&record.State,
			&record.ParamHeaders,
			&record.ParamData,
			&record.ValueHeaders,
			&record.ValueData,
			&record.Timeout,
			&record.IdempotencyKeyForCreate,
			&record.IdempotencyKeyForComplete,
			&record.Tags,
			&record.CreatedOn,
			&record.CompletedOn,
			&record.SortId,
		); err != nil {
			return nil, err
		}

		records = append(records, record)
		lastSortId = record.SortId
		rowsReturned++
	}

	return &t_aio.QueryPromisesResult{
		RowsReturned: rowsReturned,
		LastSortId:   lastSortId,
		Records:      records,
	}, nil
}

func (w *PostgresStoreWorker) searchPromises(tx *sql.Tx, cmd *t_aio.SearchPromisesCommand) (*t_aio.QueryPromisesResult, error) {
	util.Assert(cmd.Id != "", "query cannot be empty")
	util.Assert(cmd.States != nil, "states cannot be empty")
	util.Assert(cmd.Tags != nil, "tags cannot be empty")

	// convert query
	id := strings.ReplaceAll(cmd.Id, "*", "%")

	// convert list of state to bit mask
	mask := 0
	for _, state := range cmd.States {
		mask = mask | int(state)
	}

	// tags
	var tags *string

	if len(cmd.Tags) > 0 {
		t, err := json.Marshal(cmd.Tags)
		if err != nil {
			return nil, err
		}

		tags = util.ToPointer(string(t))
	}

	args := []any{
		cmd.SortId,
		id,
		mask,
		tags,
		cmd.Limit,
	}

	// select
	rows, err := tx.Query(PROMISE_SEARCH_STATEMENT, args...)
	if err != nil {
		return nil, err
	}

	rowsReturned := int64(0)
	var records []*promise.PromiseRecord
	var lastSortId int64

	for rows.Next() {
		record := &promise.PromiseRecord{}
		if err := rows.Scan(
			&record.Id,
			&record.State,
			&record.ParamHeaders,
			&record.ParamData,
			&record.ValueHeaders,
			&record.ValueData,
			&record.Timeout,
			&record.IdempotencyKeyForCreate,
			&record.IdempotencyKeyForComplete,
			&record.Tags,
			&record.CreatedOn,
			&record.CompletedOn,
			&record.SortId,
		); err != nil {
			return nil, err
		}

		records = append(records, record)
		lastSortId = record.SortId
		rowsReturned++
	}

	return &t_aio.QueryPromisesResult{
		RowsReturned: rowsReturned,
		LastSortId:   lastSortId,
		Records:      records,
	}, nil
}

func (w *PostgresStoreWorker) createPromise(_ *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreatePromiseCommand) (*t_aio.AlterPromisesResult, error) {
	util.Assert(cmd.Param.Headers != nil, "param headers must not be nil")
	util.Assert(cmd.Param.Data != nil, "param data must not be nil")
	util.Assert(cmd.Tags != nil, "tags must not be nil")

	headers, err := json.Marshal(cmd.Param.Headers)
	if err != nil {
		return nil, err
	}

	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return nil, err
	}

	// insert
	res, err := stmt.Exec(cmd.Id, headers, cmd.Param.Data, cmd.Timeout, cmd.IdempotencyKey, tags, cmd.CreatedOn)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterPromisesResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) createPromiseAndTask(tx *sql.Tx, promiseStmt *sql.Stmt, taskStmt *sql.Stmt, cmd *t_aio.CreatePromiseAndTaskCommand) (*t_aio.AlterPromisesResult, error) {
	res, err := w.createPromise(tx, promiseStmt, cmd.PromiseCommand)
	if err != nil {
		return nil, err
	}

	if res.RowsAffected == 1 {
		if _, err := w.createTask(tx, taskStmt, cmd.TaskCommand); err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (w *PostgresStoreWorker) updatePromise(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.UpdatePromiseCommand) (*t_aio.AlterPromisesResult, error) {
	util.Assert(cmd.State.In(promise.Resolved|promise.Rejected|promise.Canceled|promise.Timedout), "state must be canceled, resolved, rejected, or timedout")
	util.Assert(cmd.Value.Headers != nil, "value headers must not be nil")
	util.Assert(cmd.Value.Data != nil, "value data must not be nil")

	headers, err := json.Marshal(cmd.Value.Headers)
	if err != nil {
		return nil, err
	}

	// update
	res, err := stmt.Exec(cmd.State, headers, cmd.Value.Data, cmd.IdempotencyKey, cmd.CompletedOn, cmd.Id)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterPromisesResult{
		RowsAffected: rowsAffected,
	}, nil
}

// Callbacks

func (w *PostgresStoreWorker) createCallback(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreateCallbackCommand) (*t_aio.AlterCallbacksResult, error) {
	util.Assert(cmd.Recv != nil, "recv must not be nil")
	util.Assert(cmd.Mesg != nil, "mesg must not be nil")

	mesg, err := json.Marshal(cmd.Mesg)
	if err != nil {
		return nil, err
	}

	res, err := stmt.Exec(cmd.Id, cmd.PromiseId, cmd.Mesg.Root, cmd.Recv, mesg, cmd.Timeout, cmd.CreatedOn)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterCallbacksResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) deleteCallbacks(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.DeleteCallbacksCommand) (*t_aio.AlterCallbacksResult, error) {
	res, err := stmt.Exec(cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterCallbacksResult{
		RowsAffected: rowsAffected,
	}, nil
}

// Schedules

func (w *PostgresStoreWorker) readSchedule(tx *sql.Tx, cmd *t_aio.ReadScheduleCommand) (*t_aio.QuerySchedulesResult, error) {
	row := tx.QueryRow(SCHEDULE_SELECT_STATEMENT, cmd.Id)
	record := &schedule.ScheduleRecord{}
	rowsReturned := int64(1)

	if err := row.Scan(
		&record.Id,
		&record.Description,
		&record.Cron,
		&record.Tags,
		&record.PromiseId,
		&record.PromiseTimeout,
		&record.PromiseParamHeaders,
		&record.PromiseParamData,
		&record.PromiseTags,
		&record.LastRunTime,
		&record.NextRunTime,
		&record.IdempotencyKey,
		&record.CreatedOn,
	); err != nil {
		if err == sql.ErrNoRows {
			rowsReturned = 0
		} else {
			return nil, err
		}
	}

	var records []*schedule.ScheduleRecord
	if rowsReturned == 1 {
		records = append(records, record)
	}

	return &t_aio.QuerySchedulesResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *PostgresStoreWorker) readSchedules(tx *sql.Tx, cmd *t_aio.ReadSchedulesCommand) (*t_aio.QuerySchedulesResult, error) {
	rows, err := tx.Query(SCHEDULE_SELECT_ALL_STATEMENT, cmd.NextRunTime, cmd.Limit)
	if err != nil {
		return nil, err
	}

	rowsReturned := int64(0)
	var records []*schedule.ScheduleRecord

	for rows.Next() {
		record := &schedule.ScheduleRecord{}
		if err := rows.Scan(
			&record.Id,
			&record.Cron,
			&record.PromiseId,
			&record.PromiseTimeout,
			&record.PromiseParamHeaders,
			&record.PromiseParamData,
			&record.PromiseTags,
			&record.LastRunTime,
			&record.NextRunTime,
		); err != nil {
			return nil, err
		}

		records = append(records, record)
		rowsReturned++
	}

	return &t_aio.QuerySchedulesResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *PostgresStoreWorker) searchSchedules(tx *sql.Tx, cmd *t_aio.SearchSchedulesCommand) (*t_aio.QuerySchedulesResult, error) {
	util.Assert(cmd.Id != "", "query cannot be empty")
	util.Assert(cmd.Tags != nil, "tags cannot be empty")

	// convert query
	id := strings.ReplaceAll(cmd.Id, "*", "%")

	// tags
	var tags *string
	if len(cmd.Tags) > 0 {
		t, err := json.Marshal(cmd.Tags)
		if err != nil {
			return nil, err
		}

		tags = util.ToPointer(string(t))
	}

	// select
	rows, err := tx.Query(SCHEDULE_SEARCH_STATEMENT, cmd.SortId, id, tags, cmd.Limit)
	if err != nil {
		return nil, err
	}

	rowsReturned := int64(0)
	var records []*schedule.ScheduleRecord
	var lastSortId int64

	for rows.Next() {
		record := &schedule.ScheduleRecord{}
		if err := rows.Scan(
			&record.Id,
			&record.Cron,
			&record.Tags,
			&record.LastRunTime,
			&record.NextRunTime,
			&record.IdempotencyKey,
			&record.CreatedOn,
			&record.SortId,
		); err != nil {
			return nil, err
		}

		records = append(records, record)
		lastSortId = record.SortId
		rowsReturned++
	}

	return &t_aio.QuerySchedulesResult{
		RowsReturned: rowsReturned,
		LastSortId:   lastSortId,
		Records:      records,
	}, nil
}

func (w *PostgresStoreWorker) createSchedule(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreateScheduleCommand) (*t_aio.AlterSchedulesResult, error) {
	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return nil, err
	}

	promiseParamHeaders, err := json.Marshal(cmd.PromiseParam.Headers)
	if err != nil {
		return nil, err
	}

	promiseTags, err := json.Marshal(cmd.PromiseTags)
	if err != nil {
		return nil, err
	}

	res, err := stmt.Exec(
		cmd.Id,
		cmd.Description,
		cmd.Cron,
		tags,
		cmd.PromiseId,
		cmd.PromiseTimeout,
		promiseParamHeaders,
		cmd.PromiseParam.Data,
		promiseTags,
		cmd.NextRunTime,
		cmd.IdempotencyKey,
		cmd.CreatedOn,
	)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterSchedulesResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) updateSchedule(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.UpdateScheduleCommand) (*t_aio.AlterSchedulesResult, error) {
	res, err := stmt.Exec(cmd.NextRunTime, cmd.Id, cmd.LastRunTime)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterSchedulesResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) deleteSchedule(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.DeleteScheduleCommand) (*t_aio.AlterSchedulesResult, error) {
	res, err := stmt.Exec(cmd.Id)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterSchedulesResult{
		RowsAffected: rowsAffected,
	}, nil
}

// Locks

func (w *PostgresStoreWorker) readLock(tx *sql.Tx, cmd *t_aio.ReadLockCommand) (*t_aio.QueryLocksResult, error) {
	// select
	row := tx.QueryRow(LOCK_READ_STATEMENT, cmd.ResourceId)
	record := &lock.LockRecord{}
	rowsReturned := int64(1)

	if err := row.Scan(
		&record.ResourceId,
		&record.ProcessId,
		&record.ExecutionId,
		&record.Ttl,
		&record.ExpiresAt,
	); err != nil {
		if err == sql.ErrNoRows {
			rowsReturned = 0
		} else {
			return nil, err
		}
	}

	var records []*lock.LockRecord
	if rowsReturned == 1 {
		records = append(records, record)
	}

	return &t_aio.QueryLocksResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *PostgresStoreWorker) acquireLock(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.AcquireLockCommand) (*t_aio.AlterLocksResult, error) {
	// insert
	res, err := stmt.Exec(cmd.ResourceId, cmd.ExecutionId, cmd.ProcessId, cmd.Ttl, cmd.ExpiresAt)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterLocksResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) releaseLock(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.ReleaseLockCommand) (*t_aio.AlterLocksResult, error) {
	// delete
	res, err := stmt.Exec(cmd.ResourceId, cmd.ExecutionId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterLocksResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) hearbeatLocks(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.HeartbeatLocksCommand) (*t_aio.AlterLocksResult, error) {
	// update
	res, err := stmt.Exec(cmd.Time, cmd.ProcessId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterLocksResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) timeoutLocks(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.TimeoutLocksCommand) (*t_aio.AlterLocksResult, error) {
	// delete
	res, err := stmt.Exec(cmd.Timeout)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterLocksResult{
		RowsAffected: rowsAffected,
	}, nil
}

// Tasks

func (w *PostgresStoreWorker) readTask(tx *sql.Tx, cmd *t_aio.ReadTaskCommand) (*t_aio.QueryTasksResult, error) {
	row := tx.QueryRow(TASK_SELECT_STATEMENT, cmd.Id)
	record := &task.TaskRecord{}
	rowsReturned := int64(1)

	if err := row.Scan(
		&record.Id,
		&record.ProcessId,
		&record.State,
		&record.RootPromiseId,
		&record.Recv,
		&record.Mesg,
		&record.Timeout,
		&record.Counter,
		&record.Attempt,
		&record.Ttl,
		&record.ExpiresAt,
		&record.CreatedOn,
		&record.CompletedOn,
	); err != nil {
		if err == sql.ErrNoRows {
			rowsReturned = 0
		} else {
			return nil, store.StoreErr(err)
		}
	}

	var records []*task.TaskRecord
	if rowsReturned == 1 {
		records = append(records, record)
	}

	return &t_aio.QueryTasksResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *PostgresStoreWorker) readTasks(tx *sql.Tx, cmd *t_aio.ReadTasksCommand) (*t_aio.QueryTasksResult, error) {
	util.Assert(len(cmd.States) > 0, "must provide at least one state")

	var states task.State
	for _, state := range cmd.States {
		states |= state
	}

	rows, err := tx.Query(TASK_SELECT_ALL_STATEMENT, states, cmd.Time, cmd.Limit)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	rowsReturned := int64(0)
	var records []*task.TaskRecord

	for rows.Next() {
		record := &task.TaskRecord{}
		if err := rows.Scan(
			&record.Id,
			&record.ProcessId,
			&record.State,
			&record.RootPromiseId,
			&record.Recv,
			&record.Mesg,
			&record.Timeout,
			&record.Counter,
			&record.Attempt,
			&record.Ttl,
			&record.ExpiresAt,
			&record.CreatedOn,
			&record.CompletedOn,
		); err != nil {
			return nil, store.StoreErr(err)
		}

		records = append(records, record)
		rowsReturned++
	}

	return &t_aio.QueryTasksResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *PostgresStoreWorker) readEnqueueableTasks(tx *sql.Tx, cmd *t_aio.ReadEnqueueableTasksCommand) (*t_aio.QueryTasksResult, error) {
	rows, err := tx.Query(TASK_SELECT_ENQUEUEABLE_STATEMENT, cmd.Limit)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	rowsReturned := int64(0)
	var records []*task.TaskRecord

	for rows.Next() {
		record := &task.TaskRecord{}
		if err := rows.Scan(
			&record.Id,
			&record.ProcessId,
			&record.State,
			&record.RootPromiseId,
			&record.Recv,
			&record.Mesg,
			&record.Timeout,
			&record.Counter,
			&record.Attempt,
			&record.Ttl,
			&record.ExpiresAt,
			&record.CreatedOn,
			&record.CompletedOn,
		); err != nil {
			return nil, store.StoreErr(err)
		}

		records = append(records, record)
		rowsReturned++
	}

	return &t_aio.QueryTasksResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *PostgresStoreWorker) createTask(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreateTaskCommand) (*t_aio.AlterTasksResult, error) {
	util.Assert(cmd.Recv != nil, "recv must not be nil")
	util.Assert(cmd.Mesg != nil, "mesg must not be nil")
	util.Assert(cmd.State.In(task.Init|task.Claimed), "state must be init or claimed")
	util.Assert(cmd.State != task.Claimed || cmd.ProcessId != nil, "process id must be set if state is claimed")

	mesg, err := json.Marshal(cmd.Mesg)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	// insert
	res, err := stmt.Exec(cmd.Id, cmd.Recv, mesg, cmd.Timeout, cmd.ProcessId, cmd.State, cmd.Mesg.Root, cmd.Ttl, cmd.ExpiresAt, cmd.CreatedOn)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterTasksResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) createTasks(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreateTasksCommand) (*t_aio.AlterTasksResult, error) {
	res, err := stmt.Exec(cmd.CreatedOn, cmd.PromiseId)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, store.StoreErr(err)
	}

	return &t_aio.AlterTasksResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) completeTasks(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CompleteTasksCommand) (*t_aio.AlterTasksResult, error) {
	res, err := stmt.Exec(cmd.CompletedOn, cmd.RootPromiseId)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, store.StoreErr(err)
	}

	return &t_aio.AlterTasksResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) updateTask(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.UpdateTaskCommand) (*t_aio.AlterTasksResult, error) {
	util.Assert(len(cmd.CurrentStates) > 0, "must provide at least one current state")

	var currentStates task.State
	for _, state := range cmd.CurrentStates {
		currentStates |= state
	}

	res, err := stmt.Exec(
		cmd.ProcessId,
		cmd.State,
		cmd.Counter,
		cmd.Attempt,
		cmd.Ttl,
		cmd.ExpiresAt,
		cmd.CompletedOn,
		cmd.Id,
		currentStates,
		cmd.CurrentCounter,
	)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, store.StoreErr(err)
	}

	return &t_aio.AlterTasksResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (w *PostgresStoreWorker) heartbeatTasks(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.HeartbeatTasksCommand) (*t_aio.AlterTasksResult, error) {
	res, err := stmt.Exec(cmd.Time, cmd.ProcessId)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, store.StoreErr(err)
	}

	return &t_aio.AlterTasksResult{
		RowsAffected: rowsAffected,
	}, nil
}
