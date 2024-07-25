package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"

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
	CREATE INDEX IF NOT EXISTS idx_promises_invocation ON promises((tags->>'resonate:invocation'));
	CREATE INDEX IF NOT EXISTS idx_promises_timeout ON promises((tags->>'resonate:timeout'));

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
		resource_id            TEXT,
		execution_id           TEXT,
		process_id             TEXT,
		expiry_in_milliseconds BIGINT,
		timeout                BIGINT,
		PRIMARY KEY(resource_id)
	);

	CREATE INDEX IF NOT EXISTS idx_locks_acquire_id ON locks(resource_id, execution_id);
	CREATE INDEX IF NOT EXISTS idx_locks_heartbeat_id ON locks(process_id);
	CREATE INDEX IF NOT EXISTS idx_locks_timeout ON locks(timeout);

	CREATE TABLE IF NOT EXISTS migrations (
		id    INTEGER,
		PRIMARY KEY(id)
	);

	INSERT INTO migrations (id) VALUES (1) ON CONFLICT(id) DO NOTHING;`

	DROP_TABLE_STATEMENT = `
	DROP TABLE notifications;
	DROP TABLE subscriptions;
	DROP TABLE schedules;
	DROP TABLE timeouts;
	DROP TABLE promises;
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
		(id, state, param_headers, param_data, timeout, idempotency_key_for_create, tags, created_on)
	VALUES
		($1, $2, $3, $4, $5, $6, $7, $8)
	ON CONFLICT(id) DO NOTHING`

	PROMISE_UPDATE_STATEMENT = `
	UPDATE
		promises
	SET
		state = $1, value_headers = $2, value_data = $3, idempotency_key_for_complete = $4, completed_on = $5
	WHERE
		id = $6 AND state = 1`

	PROMISE_UPDATE_TIMEOUT_STATEMENT = `
	UPDATE
		promises
	SET
		state = CASE
					WHEN tags ->> 'resonate:timeout' IS NOT NULL AND tags ->> 'resonate:timeout' = 'true' THEN 2
					ELSE 16
				END,
		completed_on = timeout
	WHERE
		state = 1 AND timeout <= $1`

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
		resource_id, process_id, execution_id, expiry_in_milliseconds, timeout
	FROM
		locks
	WHERE
		resource_id = $1`

	LOCK_ACQUIRE_STATEMENT = `
	INSERT INTO locks
		(resource_id, execution_id, process_id, expiry_in_milliseconds, timeout)
	VALUES
		($1, $2, $3, $4, $5)
	ON CONFLICT(resource_id)
	DO UPDATE SET
		process_id = EXCLUDED.process_id,
		expiry_in_milliseconds = EXCLUDED.expiry_in_milliseconds,
		timeout = EXCLUDED.timeout
	WHERE locks.execution_id = EXCLUDED.execution_id`

	LOCK_RELEASE_STATEMENT = `
	DELETE FROM locks WHERE resource_id = $1 AND execution_id = $2`

	LOCK_HEARTBEAT_STATEMENT = `
	UPDATE
		locks
	SET
		timeout = $1 + expiry_in_milliseconds
	WHERE
		process_id = $2`

	LOCK_TIMEOUT_STATEMENT = `
	DELETE FROM locks WHERE timeout <= $1`
)

type Config struct {
	Host      string
	Port      string
	Username  string
	Password  string
	Database  string
	Query     map[string]string
	TxTimeout time.Duration
	Reset     bool
}

type PostgresStore struct {
	config *Config
	db     *sql.DB
}

type PostgresStoreWorker struct {
	*PostgresStore
	i int
}

func New(config *Config, workers int) (aio.Subsystem, error) {
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

	db.SetMaxOpenConns(workers)
	db.SetMaxIdleConns(workers)
	db.SetConnMaxIdleTime(0)

	return &PostgresStore{
		config: config,
		db:     db,
	}, nil
}

func (s *PostgresStore) String() string {
	return "store:postgres"
}

func (s *PostgresStore) Start() error {
	if _, err := s.db.Exec(CREATE_TABLE_STATEMENT); err != nil {
		return err
	}

	return nil
}

func (s *PostgresStore) Stop() error {

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

func (s *PostgresStore) NewWorker(i int) aio.Worker {
	return &PostgresStoreWorker{
		PostgresStore: s,
		i:             i,
	}
}

func (w *PostgresStoreWorker) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	return store.Process(w, sqes)
}

func (w *PostgresStoreWorker) Execute(transactions []*t_aio.Transaction) ([][]*t_aio.Result, error) {
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

func (w *PostgresStoreWorker) performCommands(tx *sql.Tx, transactions []*t_aio.Transaction) ([][]*t_aio.Result, error) {
	// Lazily defined prepared statements
	var promiseInsertStmt *sql.Stmt
	var promiseUpdateStmt *sql.Stmt
	var promiseUpdateTimeoutStmt *sql.Stmt
	var scheduleInsertStmt *sql.Stmt
	var scheduleUpdateStmt *sql.Stmt
	var scheduleDeleteStmt *sql.Stmt
	var lockAcquireStmt *sql.Stmt
	var lockReleaseStmt *sql.Stmt
	var lockHeartbeatStmt *sql.Stmt
	var lockTimeoutStmt *sql.Stmt

	// Results
	results := make([][]*t_aio.Result, len(transactions))

	for i, transaction := range transactions {
		util.Assert(len(transaction.Commands) > 0, "expected a command")
		results[i] = make([]*t_aio.Result, len(transaction.Commands))

		for j, command := range transaction.Commands {
			var err error

			switch command.Kind {
			// Promise
			case t_aio.ReadPromise:
				util.Assert(command.ReadPromise != nil, "command must not be nil")
				results[i][j], err = w.readPromise(tx, command.ReadPromise)
			case t_aio.SearchPromises:
				util.Assert(command.SearchPromises != nil, "command must not be nil")
				results[i][j], err = w.searchPromises(tx, command.SearchPromises)
			case t_aio.CreatePromise:
				if promiseInsertStmt == nil {
					promiseInsertStmt, err = tx.Prepare(PROMISE_INSERT_STATEMENT)
					if err != nil {
						return nil, err
					}
					defer promiseInsertStmt.Close()
				}

				util.Assert(command.CreatePromise != nil, "command must not be nil")
				results[i][j], err = w.createPromise(tx, promiseInsertStmt, command.CreatePromise)
			case t_aio.UpdatePromise:
				if promiseUpdateStmt == nil {
					promiseUpdateStmt, err = tx.Prepare(PROMISE_UPDATE_STATEMENT)
					if err != nil {
						return nil, err
					}
					defer promiseUpdateStmt.Close()
				}

				util.Assert(command.UpdatePromise != nil, "command must not be nil")
				results[i][j], err = w.updatePromise(tx, promiseUpdateStmt, command.UpdatePromise)
			case t_aio.TimeoutPromises:
				if promiseUpdateTimeoutStmt == nil {
					promiseUpdateTimeoutStmt, err = tx.Prepare(PROMISE_UPDATE_TIMEOUT_STATEMENT)
					if err != nil {
						return nil, err
					}
					defer promiseUpdateTimeoutStmt.Close()
				}

				util.Assert(command.TimeoutPromises != nil, "command must not be nil")
				results[i][j], err = w.timeoutPromises(tx, promiseUpdateTimeoutStmt, command.TimeoutPromises)

			// Schedule
			case t_aio.ReadSchedule:
				util.Assert(command.ReadSchedule != nil, "command must not be nil")
				results[i][j], err = w.readSchedule(tx, command.ReadSchedule)
			case t_aio.ReadSchedules:
				util.Assert(command.ReadSchedules != nil, "command must not be nil")
				results[i][j], err = w.readSchedules(tx, command.ReadSchedules)
			case t_aio.SearchSchedules:
				util.Assert(command.SearchSchedules != nil, "command must not be nil")
				results[i][j], err = w.searchSchedules(tx, command.SearchSchedules)
			case t_aio.CreateSchedule:
				if scheduleInsertStmt == nil {
					scheduleInsertStmt, err = tx.Prepare(SCHEDULE_INSERT_STATEMENT)
					if err != nil {
						return nil, err
					}
					defer scheduleInsertStmt.Close()
				}

				util.Assert(command.CreateSchedule != nil, "command must not be nil")
				results[i][j], err = w.createSchedule(tx, scheduleInsertStmt, command.CreateSchedule)
			case t_aio.UpdateSchedule:
				if scheduleUpdateStmt == nil {
					scheduleUpdateStmt, err = tx.Prepare(SCHEDULE_UPDATE_STATEMENT)
					if err != nil {
						return nil, err
					}
					defer scheduleUpdateStmt.Close()
				}

				util.Assert(command.UpdateSchedule != nil, "command must not be nil")
				results[i][j], err = w.updateSchedule(tx, scheduleUpdateStmt, command.UpdateSchedule)
			case t_aio.DeleteSchedule:
				if scheduleDeleteStmt == nil {
					scheduleDeleteStmt, err = tx.Prepare(SCHEDULE_DELETE_STATEMENT)
					if err != nil {
						return nil, err
					}
					defer scheduleDeleteStmt.Close()
				}

				util.Assert(command.DeleteSchedule != nil, "command must not be nil")
				results[i][j], err = w.deleteSchedule(tx, scheduleDeleteStmt, command.DeleteSchedule)

			// Lock
			case t_aio.ReadLock:
				util.Assert(command.ReadLock != nil, "command must not be nil")
				results[i][j], err = w.readLock(tx, command.ReadLock)
			case t_aio.AcquireLock:
				if lockAcquireStmt == nil {
					lockAcquireStmt, err = tx.Prepare(LOCK_ACQUIRE_STATEMENT)
					if err != nil {
						return nil, err
					}
					defer lockAcquireStmt.Close()
				}

				util.Assert(command.AcquireLock != nil, "command must not be nil")
				results[i][j], err = w.acquireLock(tx, lockAcquireStmt, command.AcquireLock)
			case t_aio.ReleaseLock:
				if lockReleaseStmt == nil {
					lockReleaseStmt, err = tx.Prepare(LOCK_RELEASE_STATEMENT)
					if err != nil {
						return nil, err
					}
					defer lockReleaseStmt.Close()
				}

				util.Assert(command.ReleaseLock != nil, "command must not be nil")
				results[i][j], err = w.releaseLock(tx, lockReleaseStmt, command.ReleaseLock)
			case t_aio.HeartbeatLocks:
				if lockHeartbeatStmt == nil {
					lockHeartbeatStmt, err = tx.Prepare(LOCK_HEARTBEAT_STATEMENT)
					if err != nil {
						return nil, err
					}
					defer lockHeartbeatStmt.Close()
				}

				util.Assert(command.HeartbeatLocks != nil, "command must not be nil")
				results[i][j], err = w.hearbeatLocks(tx, lockHeartbeatStmt, command.HeartbeatLocks)
			case t_aio.TimeoutLocks:
				if lockTimeoutStmt == nil {
					lockTimeoutStmt, err = tx.Prepare(LOCK_TIMEOUT_STATEMENT)
					if err != nil {
						return nil, err
					}
					defer lockTimeoutStmt.Close()
				}

				util.Assert(command.TimeoutLocks != nil, "command must not be nil")
				results[i][j], err = w.timeoutLocks(tx, lockTimeoutStmt, command.TimeoutLocks)

			default:
				panic(fmt.Sprintf("invalid command: %s", command.Kind.String()))
			}

			if err != nil {
				return nil, err
			}
		}
	}

	return results, nil
}

// PROMISES

func (w *PostgresStoreWorker) readPromise(tx *sql.Tx, cmd *t_aio.ReadPromiseCommand) (*t_aio.Result, error) {
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

	return &t_aio.Result{
		Kind: t_aio.ReadPromise,
		ReadPromise: &t_aio.QueryPromisesResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *PostgresStoreWorker) searchPromises(tx *sql.Tx, cmd *t_aio.SearchPromisesCommand) (*t_aio.Result, error) {
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
	defer rows.Close()

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

	return &t_aio.Result{
		Kind: t_aio.SearchPromises,
		SearchPromises: &t_aio.QueryPromisesResult{
			RowsReturned: rowsReturned,
			LastSortId:   lastSortId,
			Records:      records,
		},
	}, nil
}

func (w *PostgresStoreWorker) createPromise(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreatePromiseCommand) (*t_aio.Result, error) {
	util.Assert(cmd.State.In(promise.Pending|promise.Timedout), "state must be pending or timedout")
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
	res, err := stmt.Exec(cmd.Id, cmd.State, headers, cmd.Param.Data, cmd.Timeout, cmd.IdempotencyKey, tags, cmd.CreatedOn)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.CreatePromise,
		CreatePromise: &t_aio.AlterPromisesResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) updatePromise(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.UpdatePromiseCommand) (*t_aio.Result, error) {
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

	return &t_aio.Result{
		Kind: t_aio.UpdatePromise,
		UpdatePromise: &t_aio.AlterPromisesResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) timeoutPromises(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.TimeoutPromisesCommand) (*t_aio.Result, error) {
	util.Assert(cmd.Time >= 0, "time must be non-negative")

	// udpate promises
	res, err := stmt.Exec(cmd.Time)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.TimeoutPromises,
		TimeoutPromises: &t_aio.AlterPromisesResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

// SCHEDULES

func (w *PostgresStoreWorker) readSchedule(tx *sql.Tx, cmd *t_aio.ReadScheduleCommand) (*t_aio.Result, error) {
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

	return &t_aio.Result{
		Kind: t_aio.ReadSchedule,
		ReadSchedule: &t_aio.QuerySchedulesResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *PostgresStoreWorker) readSchedules(tx *sql.Tx, cmd *t_aio.ReadSchedulesCommand) (*t_aio.Result, error) {
	rows, err := tx.Query(SCHEDULE_SELECT_ALL_STATEMENT, cmd.NextRunTime, cmd.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

	return &t_aio.Result{
		Kind: t_aio.ReadSchedules,
		ReadSchedules: &t_aio.QuerySchedulesResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *PostgresStoreWorker) searchSchedules(tx *sql.Tx, cmd *t_aio.SearchSchedulesCommand) (*t_aio.Result, error) {
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
	defer rows.Close()

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

	return &t_aio.Result{
		Kind: t_aio.SearchSchedules,
		SearchSchedules: &t_aio.QuerySchedulesResult{
			RowsReturned: rowsReturned,
			LastSortId:   lastSortId,
			Records:      records,
		},
	}, nil
}

func (w *PostgresStoreWorker) createSchedule(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreateScheduleCommand) (*t_aio.Result, error) {
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

	return &t_aio.Result{
		Kind: t_aio.CreateSchedule,
		CreateSchedule: &t_aio.AlterSchedulesResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) updateSchedule(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.UpdateScheduleCommand) (*t_aio.Result, error) {
	res, err := stmt.Exec(cmd.NextRunTime, cmd.Id, cmd.LastRunTime)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.UpdateSchedule,
		UpdateSchedule: &t_aio.AlterSchedulesResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) deleteSchedule(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.DeleteScheduleCommand) (*t_aio.Result, error) {
	res, err := stmt.Exec(cmd.Id)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.DeleteSchedule,
		DeleteSchedule: &t_aio.AlterSchedulesResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

// LOCKS

func (w *PostgresStoreWorker) readLock(tx *sql.Tx, cmd *t_aio.ReadLockCommand) (*t_aio.Result, error) {
	// select
	row := tx.QueryRow(LOCK_READ_STATEMENT, cmd.ResourceId)
	record := &lock.LockRecord{}
	rowsReturned := int64(1)

	if err := row.Scan(
		&record.ResourceId,
		&record.ProcessId,
		&record.ExecutionId,
		&record.ExpiryInMilliseconds,
		&record.Timeout,
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

	return &t_aio.Result{
		Kind: t_aio.ReadLock,
		ReadLock: &t_aio.QueryLocksResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *PostgresStoreWorker) acquireLock(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.AcquireLockCommand) (*t_aio.Result, error) {
	// insert
	res, err := stmt.Exec(cmd.ResourceId, cmd.ExecutionId, cmd.ProcessId, cmd.ExpiryInMilliseconds, cmd.Timeout)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.AcquireLock,
		AcquireLock: &t_aio.AlterLocksResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) releaseLock(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.ReleaseLockCommand) (*t_aio.Result, error) {
	// delete
	res, err := stmt.Exec(cmd.ResourceId, cmd.ExecutionId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.ReleaseLock,
		ReleaseLock: &t_aio.AlterLocksResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) hearbeatLocks(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.HeartbeatLocksCommand) (*t_aio.Result, error) {
	// update
	res, err := stmt.Exec(cmd.Time, cmd.ProcessId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.HeartbeatLocks,
		HeartbeatLocks: &t_aio.AlterLocksResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) timeoutLocks(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.TimeoutLocksCommand) (*t_aio.Result, error) {
	// delete
	res, err := stmt.Exec(cmd.Timeout)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.TimeoutLocks,
		TimeoutLocks: &t_aio.AlterLocksResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}
