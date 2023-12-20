package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/notification"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/subscription"
	"github.com/resonatehq/resonate/pkg/timeout"

	_ "github.com/mattn/go-sqlite3"
)

const (
	CREATE_TABLE_STATEMENT = `
	CREATE TABLE IF NOT EXISTS promises (
		id                           TEXT UNIQUE,
		sort_id                      INTEGER PRIMARY KEY AUTOINCREMENT,
		state                        INTEGER DEFAULT 1,
		param_headers                BLOB,
		param_data                   BLOB,
		value_headers                BLOB,
		value_data                   BLOB,
		timeout                      INTEGER,
		idempotency_key_for_create   TEXT,
		idempotency_key_for_complete TEXT,
		tags                         BLOB,
		invocation                   TEXT GENERATED ALWAYS AS (json_extract(tags, '$.resonate:invocation')) STORED,
		created_on                   INTEGER,
		completed_on                 INTEGER
	);

	CREATE INDEX IF NOT EXISTS idx_promises_id ON promises(id);
	CREATE INDEX IF NOT EXISTS idx_promises_invocation ON promises(invocation);

	CREATE TABLE IF NOT EXISTS schedules (
		id                         TEXT PRIMARY KEY,
		description                TEXT,
		cron                       TEXT,
		promise_id                 TEXT,
		promise_param_headers      BLOB,
		promise_param_data         BLOB,
		promise_timeout            INTEGER,
		last_run_time              INTEGER,
		next_run_time              INTEGER,
		created_on                 INTEGER,
		idempotency_key            TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_next_run_time ON schedules (next_run_time);

	CREATE TABLE IF NOT EXISTS timeouts (
		id   TEXT,
		time INTEGER,
		PRIMARY KEY(id)
	);

	CREATE TABLE IF NOT EXISTS subscriptions (
		id           TEXT,
		promise_id   TEXT,
		sort_id      INTEGER PRIMARY KEY AUTOINCREMENT,
		url          TEXT,
		retry_policy BLOB,
		created_on   INTEGER,
		UNIQUE(id, promise_id)
	);

	CREATE INDEX IF NOT EXISTS idx_subscriptions_id ON subscriptions(id);

	CREATE TABLE IF NOT EXISTS notifications (
		id           TEXT,
		promise_id   TEXT,
		url          TEXT,
		retry_policy BLOB,
		time         INTEGER,
		attempt      INTEGER,
		PRIMARY KEY(id, promise_id)
	);`

	PROMISE_SELECT_STATEMENT = `
	SELECT
		id, state, param_headers, param_data, value_headers, value_data, timeout, idempotency_key_for_create, idempotency_key_for_complete, tags, created_on, completed_on
	FROM
		promises
	WHERE
		id = ?`

	PROMISE_SEARCH_STATEMENT = `
	SELECT
		id, state, param_headers, param_data, value_headers, value_data, timeout, idempotency_key_for_create, idempotency_key_for_complete, tags, created_on, completed_on, sort_id
	FROM
		promises
	WHERE
		(? IS NULL OR sort_id < ?) AND
		id LIKE ? AND
		state & ? != 0 AND
		(? IS NULL OR invocation = ?)
		%s
	ORDER BY
		sort_id DESC
	LIMIT
		?`

	PROMISE_INSERT_STATEMENT = `
	INSERT INTO promises
		(id, state, param_headers, param_data, timeout, idempotency_key_for_create, tags, created_on)
	VALUES
		(?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(id) DO NOTHING`

	PROMISE_UPDATE_STATEMENT = `
	UPDATE
		promises
	SET
		state = ?, value_headers = ?, value_data = ?, idempotency_key_for_complete = ?, completed_on = ?
	WHERE
		id = ? AND state = 1`

	PROMISE_UPDATE_TIMEOUT_STATEMENT = `
	UPDATE
		promises
	SET
		state = 8, completed_on = timeout
	WHERE
		state = 1 AND timeout <= ?`

	SCHEDULE_SELECT_STATEMENT = `
	SELECT
		id, description, cron, promise_id, promise_param_headers, promise_param_data, promise_timeout, last_run_time, next_run_time, created_on, idempotency_key
	FROM
		schedules
	WHERE
		id = ?`

	SCHEDULE_SELECT_ALL_STATEMENT = `
	SELECT
		id, description, cron, promise_id, promise_param_headers, promise_param_data, promise_timeout, last_run_time, next_run_time, created_on, idempotency_key
	FROM
		schedules
	WHERE
		next_run_time <= ?`

	SCHEDULE_INSERT_STATEMENT = `
	INSERT INTO schedules
		(id, description, cron, promise_id, promise_param_headers, promise_param_data, promise_timeout, last_run_time, next_run_time, created_on, idempotency_key)
	VALUES
		(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(id) DO NOTHING`

	SCHEDULE_UPDATE_STATEMENT = `
	UPDATE
		schedules
	SET
		last_run_time = next_run_time, next_run_time = ?
	WHERE
		id = ? AND next_run_time = ?`

	SCHEDULE_DELETE_STATEMENT = `
	DELETE FROM schedules WHERE id = ?`

	TIMEOUT_SELECT_STATEMENT = `
	SELECT
		id, time
	FROM
		timeouts
	ORDER BY
		time ASC, id
	LIMIT ?`

	TIMEOUT_INSERT_STATEMENT = `
	INSERT INTO timeouts
		(id, time)
	VALUES
		(?, ?)
	ON CONFLICT(id) DO NOTHING`

	TIMEOUT_DELETE_STATEMENT = `
	DELETE FROM timeouts WHERE id = ?`

	SUBSCRIPTION_SELECT_STATEMENT = `
	SELECT
		id, promise_id, url, retry_policy, created_on
	FROM
		subscriptions
	WHERE
		id = ? AND promise_id = ?`

	SUBSCRIPTION_SELECT_ALL_STATEMENT = `
	SELECT
		id, promise_id, url, retry_policy, created_on, sort_id
	FROM
		subscriptions
	WHERE
		(? IS NULL OR sort_id < ?) AND
		promise_id = ?
	ORDER BY
		sort_id DESC
	LIMIT
		?`

	SUBSCRIPTION_INSERT_STATEMENT = `
	INSERT INTO subscriptions
		(id, promise_id, url, retry_policy, created_on)
	VALUES
		(?, ?, ?, ?, ?)
	ON CONFLICT(id, promise_id) DO NOTHING`

	SUBSCRIPTION_DELETE_STATEMENT = `
	DELETE FROM subscriptions WHERE id = ? AND promise_id = ?`

	SUBSCRIPTION_DELETE_ALL_STATEMENT = `
	DELETE FROM subscriptions WHERE promise_id = ?`

	SUBSCRIPTION_DELETE_ALL_TIMEOUT_STATEMENT = `
	DELETE FROM
		subscriptions
	WHERE
		promise_id IN (SELECT id FROM promises WHERE state = 1 AND timeout <= ?)`

	NOTIFICATION_SELECT_STATEMENT = `
	SELECT
		id, promise_id, url, retry_policy, time, attempt
	FROM
		notifications
	ORDER BY
		time ASC, promise_id, id
	LIMIT ?`

	NOTIFICATION_INSERT_STATEMENT = `
	INSERT INTO notifications
		(id, promise_id, url, retry_policy, time, attempt)
	SELECT
		id, promise_id, url, retry_policy, ?, 0
	FROM
		subscriptions
	WHERE
		promise_id = ?
	ON CONFLICT(id, promise_id) DO NOTHING`

	NOTIFICATION_INSERT_TIMEOUT_STATEMENT = `
	INSERT INTO notifications
		(id, promise_id, url, retry_policy, time, attempt)
	SELECT
		id, promise_id, url, retry_policy, ?, 0
	FROM
		subscriptions
	WHERE
		promise_id IN (SELECT id FROM promises WHERE state = 1 AND timeout <= ?)
	ON CONFLICT(id, promise_id) DO NOTHING`

	NOTIFICATION_UPDATE_STATEMENT = `
	UPDATE
		notifications
	SET
		time = ?, attempt = ?
	WHERE
		id = ? AND promise_id = ?`

	NOTIFICATION_DELETE_STATEMENT = `
	DELETE FROM notifications WHERE id = ? AND promise_id = ?`
)

type Config struct {
	Path      string
	TxTimeout time.Duration
}

type SqliteStore struct {
	config *Config
	db     *sql.DB
}

type SqliteStoreWorker struct {
	*SqliteStore
}

func New(config *Config) (aio.Subsystem, error) {
	db, err := sql.Open("sqlite3", config.Path)
	if err != nil {
		return nil, err
	}

	return &SqliteStore{
		config: config,
		db:     db,
	}, nil
}

func (s *SqliteStore) String() string {
	return "store:sqlite"
}

func (s *SqliteStore) Start() error {
	if _, err := s.db.Exec(CREATE_TABLE_STATEMENT); err != nil {
		return err
	}

	return nil
}

func (s *SqliteStore) Stop() error {
	return s.db.Close()
}

func (s *SqliteStore) Reset() error {
	if _, err := os.Stat(s.config.Path); err != nil {
		return nil
	}

	return os.Remove(s.config.Path)
}

func (s *SqliteStore) NewWorker(int) aio.Worker {
	return &SqliteStoreWorker{s}
}

func (w *SqliteStoreWorker) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	return store.Process(w, sqes)
}

func (w *SqliteStoreWorker) Execute(transactions []*t_aio.Transaction) ([][]*t_aio.Result, error) {
	util.Assert(len(transactions) > 0, "expected a transaction")

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

func (w *SqliteStoreWorker) performCommands(tx *sql.Tx, transactions []*t_aio.Transaction) ([][]*t_aio.Result, error) {
	scheduleInsertStmt, err := tx.Prepare(SCHEDULE_INSERT_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer scheduleInsertStmt.Close()

	scheduleUpdateStmt, err := tx.Prepare(SCHEDULE_UPDATE_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer scheduleUpdateStmt.Close()

	scheduleDeleteStmt, err := tx.Prepare(SCHEDULE_DELETE_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer scheduleDeleteStmt.Close()

	promiseInsertStmt, err := tx.Prepare(PROMISE_INSERT_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer promiseInsertStmt.Close()

	promiseUpdateStmt, err := tx.Prepare(PROMISE_UPDATE_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer promiseUpdateStmt.Close()

	promiseUpdateTimeoutStmt, err := tx.Prepare(PROMISE_UPDATE_TIMEOUT_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer promiseUpdateTimeoutStmt.Close()

	timeoutInsertStmt, err := tx.Prepare(TIMEOUT_INSERT_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer timeoutInsertStmt.Close()

	timeoutDeleteStmt, err := tx.Prepare(TIMEOUT_DELETE_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer timeoutDeleteStmt.Close()

	subscriptionInsertStmt, err := tx.Prepare(SUBSCRIPTION_INSERT_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer subscriptionInsertStmt.Close()

	subscriptionDeleteStmt, err := tx.Prepare(SUBSCRIPTION_DELETE_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer subscriptionDeleteStmt.Close()

	subscriptionDeleteAllStmt, err := tx.Prepare(SUBSCRIPTION_DELETE_ALL_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer subscriptionDeleteAllStmt.Close()

	subscriptionDeleteAllTimeoutStmt, err := tx.Prepare(SUBSCRIPTION_DELETE_ALL_TIMEOUT_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer subscriptionDeleteAllTimeoutStmt.Close()

	notificationInsertStmt, err := tx.Prepare(NOTIFICATION_INSERT_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer notificationInsertStmt.Close()

	notificationInsertTimeoutStmt, err := tx.Prepare(NOTIFICATION_INSERT_TIMEOUT_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer notificationInsertTimeoutStmt.Close()

	notificationUpdateStmt, err := tx.Prepare(NOTIFICATION_UPDATE_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer notificationUpdateStmt.Close()

	notificationDeleteStmt, err := tx.Prepare(NOTIFICATION_DELETE_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer notificationDeleteStmt.Close()

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
				util.Assert(command.CreatePromise != nil, "command must not be nil")
				results[i][j], err = w.createPromise(tx, promiseInsertStmt, command.CreatePromise)
			case t_aio.UpdatePromise:
				util.Assert(command.UpdatePromise != nil, "command must not be nil")
				results[i][j], err = w.updatePromise(tx, promiseUpdateStmt, command.UpdatePromise)
			case t_aio.TimeoutPromises:
				util.Assert(command.TimeoutPromises != nil, "command must not be nil")
				results[i][j], err = w.timeoutPromises(tx, promiseUpdateTimeoutStmt, command.TimeoutPromises)

			// Schedule
			case t_aio.ReadSchedule:
				util.Assert(command.ReadSchedule != nil, "command must not be nil")
				results[i][j], err = w.readSchedule(tx, command.ReadSchedule)
			case t_aio.ReadSchedules:
				util.Assert(command.ReadSchedules != nil, "command must not be nil")
				results[i][j], err = w.readSchedules(tx, command.ReadSchedules)
			case t_aio.CreateSchedule:
				util.Assert(command.CreateSchedule != nil, "command must not be nil")
				results[i][j], err = w.createSchedule(tx, scheduleInsertStmt, command.CreateSchedule)
			case t_aio.UpdateSchedule:
				util.Assert(command.UpdateSchedule != nil, "command must not be nil")
				results[i][j], err = w.updateSchedule(tx, scheduleUpdateStmt, command.UpdateSchedule)
			case t_aio.DeleteSchedule:
				util.Assert(command.DeleteSchedule != nil, "command must not be nil")
				results[i][j], err = w.deleteSchedule(tx, scheduleDeleteStmt, command.DeleteSchedule)

			// Timeout
			case t_aio.ReadTimeouts:
				util.Assert(command.ReadTimeouts != nil, "command must not be nil")
				results[i][j], err = w.readTimeouts(tx, command.ReadTimeouts)
			case t_aio.CreateTimeout:
				util.Assert(command.CreateTimeout != nil, "command must not be nil")
				results[i][j], err = w.createTimeout(tx, timeoutInsertStmt, command.CreateTimeout)
			case t_aio.DeleteTimeout:
				util.Assert(command.DeleteTimeout != nil, "command must not be nil")
				results[i][j], err = w.deleteTimeout(tx, timeoutDeleteStmt, command.DeleteTimeout)

			// Subscription
			case t_aio.ReadSubscription:
				util.Assert(command.ReadSubscription != nil, "command must not be nil")
				results[i][j], err = w.readSubscription(tx, command.ReadSubscription)
			case t_aio.ReadSubscriptions:
				util.Assert(command.ReadSubscriptions != nil, "command must not be nil")
				results[i][j], err = w.readSubscriptions(tx, command.ReadSubscriptions)
			case t_aio.CreateSubscription:
				util.Assert(command.CreateSubscription != nil, "command must not be nil")
				results[i][j], err = w.createSubscription(tx, subscriptionInsertStmt, command.CreateSubscription)
			case t_aio.DeleteSubscription:
				util.Assert(command.DeleteSubscription != nil, "command must not be nil")
				results[i][j], err = w.deleteSubscription(tx, subscriptionDeleteStmt, command.DeleteSubscription)
			case t_aio.DeleteSubscriptions:
				util.Assert(command.DeleteSubscriptions != nil, "command must not be nil")
				results[i][j], err = w.deleteSubscriptions(tx, subscriptionDeleteAllStmt, command.DeleteSubscriptions)
			case t_aio.TimeoutDeleteSubscriptions:
				util.Assert(command.TimeoutDeleteSubscriptions != nil, "command must not be nil")
				results[i][j], err = w.timeoutDeleteSubscriptions(tx, subscriptionDeleteAllTimeoutStmt, command.TimeoutDeleteSubscriptions)

			// Notification
			case t_aio.ReadNotifications:
				util.Assert(command.ReadNotifications != nil, "command must not be nil")
				results[i][j], err = w.readNotifications(tx, command.ReadNotifications)
			case t_aio.CreateNotifications:
				util.Assert(command.CreateNotifications != nil, "command must not be nil")
				results[i][j], err = w.createNotifications(tx, notificationInsertStmt, command.CreateNotifications)
			case t_aio.UpdateNotification:
				util.Assert(command.UpdateNotification != nil, "command must not be nil")
				results[i][j], err = w.updateNotification(tx, notificationUpdateStmt, command.UpdateNotification)
			case t_aio.DeleteNotification:
				util.Assert(command.DeleteNotification != nil, "command must not be nil")
				results[i][j], err = w.deleteNotification(tx, notificationDeleteStmt, command.DeleteNotification)
			case t_aio.TimeoutCreateNotifications:
				util.Assert(command.TimeoutCreateNotifications != nil, "command must not be nil")
				results[i][j], err = w.timeoutCreateNotifications(tx, notificationInsertTimeoutStmt, command.TimeoutCreateNotifications)

			default:
				panic("invalid command")
			}

			if err != nil {
				return nil, err
			}
		}
	}

	return results, nil
}

// Promises

func (w *SqliteStoreWorker) readPromise(tx *sql.Tx, cmd *t_aio.ReadPromiseCommand) (*t_aio.Result, error) {
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

func (w *SqliteStoreWorker) searchPromises(tx *sql.Tx, cmd *t_aio.SearchPromisesCommand) (*t_aio.Result, error) {
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
	var invocation *string
	placeholders := []string{}
	placeholderVars := []any{}

	for k, v := range cmd.Tags {
		if k == "resonate:invocation" {
			invocation = &v
			continue
		}

		placeholders = append(placeholders, "json_extract(tags, ?) = ?")
		placeholderVars = append(placeholderVars, "$."+k, v)
	}

	vars := []any{
		cmd.SortId,
		cmd.SortId,
		id,
		mask,
		invocation,
		invocation,
	}

	vars = append(vars, placeholderVars...)
	vars = append(vars, cmd.Limit)

	var placeholder string
	if len(placeholders) > 0 {
		placeholder = "AND " + strings.Join(placeholders, " AND ")
	}

	// select
	rows, err := tx.Query(fmt.Sprintf(PROMISE_SEARCH_STATEMENT, placeholder), vars...)
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

func (w *SqliteStoreWorker) createPromise(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreatePromiseCommand) (*t_aio.Result, error) {
	util.Assert(cmd.State.In(promise.Pending|promise.Timedout), "state must be pending or timedout")
	util.Assert(cmd.Param.Headers != nil, "headers must not be nil")
	util.Assert(cmd.Param.Data != nil, "data must not be nil")
	util.Assert(cmd.Tags != nil, "tags must not be nil")

	headers, err := json.Marshal(cmd.Param.Headers)
	if err != nil {
		return nil, err
	}

	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return nil, err
	}

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

func (w *SqliteStoreWorker) updatePromise(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.UpdatePromiseCommand) (*t_aio.Result, error) {
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

func (w *SqliteStoreWorker) timeoutPromises(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.TimeoutPromisesCommand) (*t_aio.Result, error) {
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

// Schedules

func (w *SqliteStoreWorker) readSchedule(tx *sql.Tx, cmd *t_aio.ReadScheduleCommand) (*t_aio.Result, error) {
	row := tx.QueryRow(SCHEDULE_SELECT_STATEMENT, cmd.Id)
	record := &schedule.ScheduleRecord{}
	rowsReturned := int64(1)

	if err := row.Scan(
		&record.Id,
		&record.Desc,
		&record.Cron,
		&record.PromiseId,
		&record.PromiseParamHeaders,
		&record.PromiseParamData,
		&record.PromiseTimeout,
		&record.LastRunTime,
		&record.NextRunTime,
		&record.CreatedOn,
		&record.IdempotencyKey,
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

func (w *SqliteStoreWorker) readSchedules(tx *sql.Tx, cmd *t_aio.ReadSchedulesCommand) (*t_aio.Result, error) {
	rows, err := tx.Query(SCHEDULE_SELECT_ALL_STATEMENT, cmd.NextRunTime)
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
			&record.Desc,
			&record.Cron,
			&record.PromiseId,
			&record.PromiseParamHeaders,
			&record.PromiseParamData,
			&record.PromiseTimeout,
			&record.LastRunTime,
			&record.NextRunTime,
			&record.CreatedOn,
			&record.IdempotencyKey,
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

func (w *SqliteStoreWorker) createSchedule(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreateScheduleCommand) (*t_aio.Result, error) {
	headers, err := json.Marshal(cmd.PromiseParam.Headers)
	if err != nil {
		return nil, err
	}

	res, err := stmt.Exec(cmd.Id, cmd.Desc, cmd.Cron, cmd.PromiseId, headers, cmd.PromiseParam.Data, cmd.PromiseTimeout, cmd.LastRunTime, cmd.NextRunTime, cmd.CreatedOn, cmd.IdempotencyKey)
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

func (w *SqliteStoreWorker) updateSchedule(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.UpdateScheduleCommand) (*t_aio.Result, error) {
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

func (w *SqliteStoreWorker) deleteSchedule(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.DeleteScheduleCommand) (*t_aio.Result, error) {
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

// Timeouts

func (w *SqliteStoreWorker) readTimeouts(tx *sql.Tx, cmd *t_aio.ReadTimeoutsCommand) (*t_aio.Result, error) {
	// select
	rows, err := tx.Query(TIMEOUT_SELECT_STATEMENT, cmd.N)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rowsReturned := int64(0)
	var records []*timeout.TimeoutRecord

	for rows.Next() {
		record := &timeout.TimeoutRecord{}
		if err := rows.Scan(&record.Id, &record.Time); err != nil {
			return nil, err
		}

		rowsReturned++
		records = append(records, record)
	}

	return &t_aio.Result{
		Kind: t_aio.ReadTimeouts,
		ReadTimeouts: &t_aio.QueryTimeoutsResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *SqliteStoreWorker) createTimeout(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreateTimeoutCommand) (*t_aio.Result, error) {
	util.Assert(cmd.Time >= 0, "time must be non-negative")

	// insert
	res, err := stmt.Exec(cmd.Id, cmd.Time)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.CreateTimeout,
		CreateTimeout: &t_aio.AlterTimeoutsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) deleteTimeout(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.DeleteTimeoutCommand) (*t_aio.Result, error) {
	// insert
	res, err := stmt.Exec(cmd.Id)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.DeleteTimeout,
		DeleteTimeout: &t_aio.AlterTimeoutsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

// Subscriptions

func (w *SqliteStoreWorker) readSubscription(tx *sql.Tx, cmd *t_aio.ReadSubscriptionCommand) (*t_aio.Result, error) {
	// select
	row := tx.QueryRow(SUBSCRIPTION_SELECT_STATEMENT, cmd.Id, cmd.PromiseId)
	record := &subscription.SubscriptionRecord{}
	rowsReturned := int64(1)

	if err := row.Scan(&record.Id, &record.PromiseId, &record.Url, &record.RetryPolicy, &record.CreatedOn); err != nil {
		if err == sql.ErrNoRows {
			rowsReturned = 0
		} else {
			return nil, err
		}
	}

	var records []*subscription.SubscriptionRecord
	if rowsReturned == 1 {
		records = append(records, record)
	}

	return &t_aio.Result{
		Kind: t_aio.ReadSubscription,
		ReadSubscription: &t_aio.QuerySubscriptionsResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *SqliteStoreWorker) readSubscriptions(tx *sql.Tx, cmd *t_aio.ReadSubscriptionsCommand) (*t_aio.Result, error) {
	// select
	rows, err := tx.Query(SUBSCRIPTION_SELECT_ALL_STATEMENT, cmd.SortId, cmd.SortId, cmd.PromiseId, cmd.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rowsReturned := int64(0)
	var records []*subscription.SubscriptionRecord
	var lastSortId int64

	for rows.Next() {
		record := &subscription.SubscriptionRecord{}
		if err := rows.Scan(&record.Id, &record.PromiseId, &record.Url, &record.RetryPolicy, &record.CreatedOn, &record.SortId); err != nil {
			return nil, err
		}

		records = append(records, record)
		lastSortId = record.SortId
		rowsReturned++
	}

	return &t_aio.Result{
		Kind: t_aio.ReadSubscriptions,
		ReadSubscriptions: &t_aio.QuerySubscriptionsResult{
			RowsReturned: rowsReturned,
			LastSortId:   lastSortId,
			Records:      records,
		},
	}, nil
}

func (w *SqliteStoreWorker) createSubscription(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreateSubscriptionCommand) (*t_aio.Result, error) {
	util.Assert(cmd.RetryPolicy != nil, "retry policy must not be nil")

	retryPolicy, err := json.Marshal(cmd.RetryPolicy)
	if err != nil {
		return nil, err
	}

	// insert
	res, err := stmt.Exec(cmd.Id, cmd.PromiseId, cmd.Url, retryPolicy, cmd.CreatedOn)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.CreateSubscription,
		CreateSubscription: &t_aio.AlterSubscriptionsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) deleteSubscription(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.DeleteSubscriptionCommand) (*t_aio.Result, error) {
	// delete
	res, err := stmt.Exec(cmd.Id, cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.DeleteSubscription,
		DeleteSubscription: &t_aio.AlterSubscriptionsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) deleteSubscriptions(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.DeleteSubscriptionsCommand) (*t_aio.Result, error) {
	// delete
	res, err := stmt.Exec(cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.DeleteSubscriptions,
		DeleteSubscriptions: &t_aio.AlterSubscriptionsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) timeoutDeleteSubscriptions(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.TimeoutDeleteSubscriptionsCommand) (*t_aio.Result, error) {
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
		Kind: t_aio.TimeoutDeleteSubscriptions,
		TimeoutDeleteSubscriptions: &t_aio.AlterSubscriptionsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

// Notifications

func (w *SqliteStoreWorker) readNotifications(tx *sql.Tx, cmd *t_aio.ReadNotificationsCommand) (*t_aio.Result, error) {
	// select
	rows, err := tx.Query(NOTIFICATION_SELECT_STATEMENT, cmd.N)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rowsReturned := int64(0)
	var records []*notification.NotificationRecord

	for rows.Next() {
		record := &notification.NotificationRecord{}
		if err := rows.Scan(&record.Id, &record.PromiseId, &record.Url, &record.RetryPolicy, &record.Time, &record.Attempt); err != nil {
			return nil, err
		}

		rowsReturned++
		records = append(records, record)
	}

	return &t_aio.Result{
		Kind: t_aio.ReadNotifications,
		ReadNotifications: &t_aio.QueryNotificationsResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *SqliteStoreWorker) createNotifications(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.CreateNotificationsCommand) (*t_aio.Result, error) {
	util.Assert(cmd.Time >= 0, "time must be non-negative")

	// insert
	res, err := stmt.Exec(cmd.Time, cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.CreateNotifications,
		CreateNotifications: &t_aio.AlterNotificationsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) updateNotification(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.UpdateNotificationCommand) (*t_aio.Result, error) {
	// update
	res, err := stmt.Exec(cmd.Time, cmd.Attempt, cmd.Id, cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.UpdateNotification,
		UpdateNotification: &t_aio.AlterNotificationsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) deleteNotification(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.DeleteNotificationCommand) (*t_aio.Result, error) {
	// insert
	res, err := stmt.Exec(cmd.Id, cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.DeleteNotification,
		DeleteNotification: &t_aio.AlterNotificationsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) timeoutCreateNotifications(tx *sql.Tx, stmt *sql.Stmt, cmd *t_aio.TimeoutCreateNotificationsCommand) (*t_aio.Result, error) {
	util.Assert(cmd.Time >= 0, "time must be non-negative")

	// udpate promises
	res, err := stmt.Exec(cmd.Time, cmd.Time)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &t_aio.Result{
		Kind: t_aio.TimeoutCreateNotifications,
		TimeoutCreateNotifications: &t_aio.AlterNotificationsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}
