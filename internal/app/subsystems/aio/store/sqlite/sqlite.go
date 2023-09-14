package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/notification"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
	"github.com/resonatehq/resonate/pkg/timeout"

	_ "github.com/mattn/go-sqlite3"
)

const (
	CREATE_TABLE_STATEMENT = `
	CREATE TABLE IF NOT EXISTS promises (
		id            TEXT,
		state         INTEGER DEFAULT 1,
		param_headers BLOB,
		param_ikey    TEXT,
		param_data    BLOB,
		value_headers BLOB,
		value_ikey    TEXT,
		value_data    BLOB,
		timeout       INTEGER,
		tags          BLOB,
		created_on    INTEGER,
		completed_on  INTEGER,
		PRIMARY KEY(id)
	);

	CREATE TABLE IF NOT EXISTS timeouts (
		id   TEXT,
		time INTEGER,
		PRIMARY KEY(id)
	);

	CREATE TABLE IF NOT EXISTS subscriptions (
		id           TEXT,
		promise_id   TEXT,
		url          TEXT,
		retry_policy BLOB,
		created_on   INTEGER,
		PRIMARY KEY(id, promise_id)
	);

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
		id, state, param_headers, param_ikey, param_data, value_headers, value_ikey, value_data, timeout, tags, created_on, completed_on
	FROM
		promises
	WHERE
		id = ?`

	PROMISE_SEARCH_STATEMENT = `
	SELECT
		id, state, param_headers, param_ikey, param_data, value_headers, value_ikey, value_data, timeout, tags, created_on, completed_on
	FROM
		promises
	WHERE
		id GLOB ? AND state = ?
	ORDER BY
		id`

	PROMISE_INSERT_STATEMENT = `
	INSERT INTO promises
		(id, state, param_headers, param_ikey, param_data, timeout, tags, created_on)
	VALUES
		(?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(id) DO NOTHING`

	PROMISE_UPDATE_STATMENT = `
	UPDATE promises
	SET state = ?, value_headers = ?, value_ikey = ?, value_data = ?, completed_on = ?
	WHERE id = ? AND state = 1`

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
		id, promise_id, url, retry_policy, created_on
	FROM
		subscriptions
	WHERE
		promise_id IN (%s)
	ORDER BY
		id, promise_id`

	SUBSCRIPTION_INSERT_STATEMENT = `
	INSERT INTO subscriptions
		(id, promise_id, url, retry_policy, created_on)
	VALUES
		(?, ?, ?, ?, ?)
	ON CONFLICT(id, promise_id) DO NOTHING`

	SUBSCRIPTION_DELETE_STATEMENT = `
	DELETE FROM subscriptions WHERE id = ? and promise_id = ?`

	SUBSCRIPTION_DELETE_ALL_STATEMENT = `
	DELETE FROM subscriptions WHERE promise_id = ?`

	NOTIFICATION_SELECT_STATEMENT = `
	SELECT
		id, promise_id, url, retry_policy, time, attempt
	FROM
		notifications
	ORDER BY
		time ASC, id, promise_id
	LIMIT ?`

	NOTIFICATION_INSERT_STATEMENT = `
	INSERT INTO notifications
		(id, promise_id, url, retry_policy, time, attempt)
	VALUES
		(?, ?, ?, ?, ?, 0)
	ON CONFLICT(id, promise_id) DO NOTHING`

	NOTIFICATION_UPDATE_STATEMENT = `
	UPDATE notifications
	SET time = ?, attempt = ?
	WHERE id = ? and promise_id = ?`

	NOTIFICATION_DELETE_STATEMENT = `
	DELETE FROM notifications WHERE id = ? and promise_id = ?`
)

type Config struct {
	Path string
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

func (w *SqliteStoreWorker) Process(sqes []*bus.SQE[types.Submission, types.Completion]) []*bus.CQE[types.Submission, types.Completion] {
	return store.Process(w, sqes)
}

func (w *SqliteStoreWorker) Execute(transactions []*types.Transaction) ([][]*types.Result, error) {
	util.Assert(len(transactions) > 0, "expected a transaction")

	tx, err := w.db.Begin()
	if err != nil {
		return nil, err
	}

	results, err := w.performCommands(tx, transactions)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return nil, err
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return results, nil
}

func (w *SqliteStoreWorker) performCommands(tx *sql.Tx, transactions []*types.Transaction) ([][]*types.Result, error) {
	promiseInsertStmt, err := tx.Prepare(PROMISE_INSERT_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer promiseInsertStmt.Close()

	promiseUpdateStmt, err := tx.Prepare(PROMISE_UPDATE_STATMENT)
	if err != nil {
		return nil, err
	}
	defer promiseUpdateStmt.Close()

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

	notificationInsertStmt, err := tx.Prepare(NOTIFICATION_INSERT_STATEMENT)
	if err != nil {
		return nil, err
	}
	defer notificationInsertStmt.Close()

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

	results := make([][]*types.Result, len(transactions))

	for i, transaction := range transactions {
		util.Assert(len(transaction.Commands) > 0, "expected a command")
		results[i] = make([]*types.Result, len(transaction.Commands))

		for j, command := range transaction.Commands {
			var err error

			switch command.Kind {
			// Promise
			case types.StoreReadPromise:
				util.Assert(command.ReadPromise != nil, "command must not be nil")
				results[i][j], err = w.readPromise(tx, command.ReadPromise)
			case types.StoreSearchPromises:
				util.Assert(command.SearchPromises != nil, "command must not be nil")
				results[i][j], err = w.searchPromises(tx, command.SearchPromises)
			case types.StoreCreatePromise:
				util.Assert(command.CreatePromise != nil, "command must not be nil")
				results[i][j], err = w.createPromise(tx, promiseInsertStmt, command.CreatePromise)
			case types.StoreUpdatePromise:
				util.Assert(command.UpdatePromise != nil, "command must not be nil")
				results[i][j], err = w.updatePromise(tx, promiseUpdateStmt, command.UpdatePromise)

			// Timeout
			case types.StoreReadTimeouts:
				util.Assert(command.ReadTimeouts != nil, "command must not be nil")
				results[i][j], err = w.readTimeouts(tx, command.ReadTimeouts)
			case types.StoreCreateTimeout:
				util.Assert(command.CreateTimeout != nil, "command must not be nil")
				results[i][j], err = w.createTimeout(tx, timeoutInsertStmt, command.CreateTimeout)
			case types.StoreDeleteTimeout:
				util.Assert(command.DeleteTimeout != nil, "command must not be nil")
				results[i][j], err = w.deleteTimeout(tx, timeoutDeleteStmt, command.DeleteTimeout)

			// Subscription
			case types.StoreReadSubscription:
				util.Assert(command.ReadSubscription != nil, "command must not be nil")
				results[i][j], err = w.readSubscription(tx, command.ReadSubscription)
			case types.StoreReadSubscriptions:
				util.Assert(command.ReadSubscriptions != nil, "command must not be nil")
				results[i][j], err = w.readSubscriptions(tx, command.ReadSubscriptions)
			case types.StoreCreateSubscription:
				util.Assert(command.CreateSubscription != nil, "command must not be nil")
				results[i][j], err = w.createSubscription(tx, subscriptionInsertStmt, command.CreateSubscription)
			case types.StoreDeleteSubscription:
				util.Assert(command.DeleteSubscription != nil, "command must not be nil")
				results[i][j], err = w.deleteSubscription(tx, subscriptionDeleteStmt, command.DeleteSubscription)
			case types.StoreDeleteSubscriptions:
				util.Assert(command.DeleteSubscriptions != nil, "command must not be nil")
				results[i][j], err = w.deleteSubscriptions(tx, subscriptionDeleteAllStmt, command.DeleteSubscriptions)

			// Notification
			case types.StoreReadNotifications:
				util.Assert(command.ReadNotifications != nil, "command must not be nil")
				results[i][j], err = w.readNotifications(tx, command.ReadNotifications)
			case types.StoreCreateNotification:
				util.Assert(command.CreateNotification != nil, "command must not be nil")
				results[i][j], err = w.createNotification(tx, notificationInsertStmt, command.CreateNotification)
			case types.StoreUpdateNotification:
				util.Assert(command.UpdateNotification != nil, "command must not be nil")
				results[i][j], err = w.updateNotification(tx, notificationUpdateStmt, command.UpdateNotification)
			case types.StoreDeleteNotification:
				util.Assert(command.DeleteNotification != nil, "command must not be nil")
				results[i][j], err = w.deleteNotification(tx, notificationDeleteStmt, command.DeleteNotification)
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

func (w *SqliteStoreWorker) readPromise(tx *sql.Tx, cmd *types.ReadPromiseCommand) (*types.Result, error) {
	// select
	row := tx.QueryRow(PROMISE_SELECT_STATEMENT, cmd.Id)
	record := &promise.PromiseRecord{}
	rowsReturned := int64(1)

	if err := row.Scan(
		&record.Id,
		&record.State,
		&record.ParamHeaders,
		&record.ParamIkey,
		&record.ParamData,
		&record.ValueHeaders,
		&record.ValueIkey,
		&record.ValueData,
		&record.Timeout,
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

	return &types.Result{
		Kind: types.StoreReadPromise,
		ReadPromise: &types.QueryPromisesResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *SqliteStoreWorker) searchPromises(tx *sql.Tx, cmd *types.SearchPromisesCommand) (*types.Result, error) {
	util.Assert(cmd.State == promise.Pending, "status must be pending")

	// select
	rows, err := tx.Query(PROMISE_SEARCH_STATEMENT, cmd.Q, cmd.State)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rowsReturned := int64(0)
	var records []*promise.PromiseRecord

	for rows.Next() {
		record := &promise.PromiseRecord{}
		if err := rows.Scan(
			&record.Id,
			&record.State,
			&record.ParamHeaders,
			&record.ParamIkey,
			&record.ParamData,
			&record.ValueHeaders,
			&record.ValueIkey,
			&record.ValueData,
			&record.Timeout,
			&record.Tags,
			&record.CreatedOn,
			&record.CompletedOn,
		); err != nil {
			return nil, err
		}

		rowsReturned++
		records = append(records, record)
	}

	return &types.Result{
		Kind: types.StoreSearchPromises,
		SearchPromises: &types.QueryPromisesResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *SqliteStoreWorker) createPromise(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreatePromiseCommand) (*types.Result, error) {
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

	// insert
	res, err := stmt.Exec(cmd.Id, promise.Pending, headers, cmd.Param.Ikey, cmd.Param.Data, cmd.Timeout, tags, cmd.CreatedOn)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &types.Result{
		Kind: types.StoreCreatePromise,
		CreatePromise: &types.AlterPromisesResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) updatePromise(tx *sql.Tx, stmt *sql.Stmt, cmd *types.UpdatePromiseCommand) (*types.Result, error) {
	util.Assert(cmd.State.In(promise.Resolved|promise.Rejected|promise.Canceled|promise.Timedout), "state must be canceled, resolved, rejected, or timedout")
	util.Assert(cmd.Value.Headers != nil, "value headers must not be nil")
	util.Assert(cmd.Value.Data != nil, "value data must not be nil")

	headers, err := json.Marshal(cmd.Value.Headers)
	if err != nil {
		return nil, err
	}

	// update
	res, err := stmt.Exec(cmd.State, headers, cmd.Value.Ikey, cmd.Value.Data, cmd.CompletedOn, cmd.Id)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &types.Result{
		Kind: types.StoreUpdatePromise,
		UpdatePromise: &types.AlterPromisesResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) readTimeouts(tx *sql.Tx, cmd *types.ReadTimeoutsCommand) (*types.Result, error) {
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

	return &types.Result{
		Kind: types.StoreReadTimeouts,
		ReadTimeouts: &types.QueryTimeoutsResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *SqliteStoreWorker) createTimeout(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreateTimeoutCommand) (*types.Result, error) {
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

	return &types.Result{
		Kind: types.StoreCreateTimeout,
		CreateTimeout: &types.AlterTimeoutsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) deleteTimeout(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteTimeoutCommand) (*types.Result, error) {
	// insert
	res, err := stmt.Exec(cmd.Id)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &types.Result{
		Kind: types.StoreDeleteTimeout,
		DeleteTimeout: &types.AlterTimeoutsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) readSubscription(tx *sql.Tx, cmd *types.ReadSubscriptionCommand) (*types.Result, error) {
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

	return &types.Result{
		Kind: types.StoreReadSubscription,
		ReadSubscription: &types.QuerySubscriptionsResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *SqliteStoreWorker) readSubscriptions(tx *sql.Tx, cmd *types.ReadSubscriptionsCommand) (*types.Result, error) {
	util.Assert(len(cmd.PromiseIds) > 0, "expected a promise id")

	// select
	var placeholders string
	promiseIds := make([]interface{}, len(cmd.PromiseIds))

	for i, promiseId := range cmd.PromiseIds {
		if i == len(cmd.PromiseIds)-1 {
			placeholders += "?"
		} else {
			placeholders += "?,"
		}

		promiseIds[i] = promiseId
	}

	stmt := fmt.Sprintf(SUBSCRIPTION_SELECT_ALL_STATEMENT, placeholders)
	rows, err := tx.Query(stmt, promiseIds...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rowsReturned := int64(0)
	var records []*subscription.SubscriptionRecord

	for rows.Next() {
		record := &subscription.SubscriptionRecord{}
		if err := rows.Scan(&record.Id, &record.PromiseId, &record.Url, &record.RetryPolicy, &record.CreatedOn); err != nil {
			return nil, err
		}

		rowsReturned++
		records = append(records, record)
	}

	return &types.Result{
		Kind: types.StoreReadSubscriptions,
		ReadSubscriptions: &types.QuerySubscriptionsResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *SqliteStoreWorker) createSubscription(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreateSubscriptionCommand) (*types.Result, error) {
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

	return &types.Result{
		Kind: types.StoreCreateSubscription,
		CreateSubscription: &types.AlterSubscriptionResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) deleteSubscription(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteSubscriptionCommand) (*types.Result, error) {
	// delete
	res, err := stmt.Exec(cmd.Id, cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &types.Result{
		Kind: types.StoreDeleteSubscription,
		DeleteSubscription: &types.AlterSubscriptionResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) deleteSubscriptions(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteSubscriptionsCommand) (*types.Result, error) {
	// delete
	res, err := stmt.Exec(cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &types.Result{
		Kind: types.StoreDeleteSubscriptions,
		DeleteSubscriptions: &types.AlterSubscriptionResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) readNotifications(tx *sql.Tx, cmd *types.ReadNotificationsCommand) (*types.Result, error) {
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

	return &types.Result{
		Kind: types.StoreReadNotifications,
		ReadNotifications: &types.QueryNotificationsResult{
			RowsReturned: rowsReturned,
			Records:      records,
		},
	}, nil
}

func (w *SqliteStoreWorker) createNotification(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreateNotificationCommand) (*types.Result, error) {
	util.Assert(cmd.Time >= 0, "time must be non-negative")
	util.Assert(cmd.RetryPolicy != nil, "retry policy must not be nil")

	// insert
	res, err := stmt.Exec(cmd.Id, cmd.PromiseId, cmd.Url, cmd.RetryPolicy, cmd.Time)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &types.Result{
		Kind: types.StoreCreateNotification,
		CreateNotification: &types.AlterNotificationsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) updateNotification(tx *sql.Tx, stmt *sql.Stmt, cmd *types.UpdateNotificationCommand) (*types.Result, error) {
	// update
	res, err := stmt.Exec(cmd.Time, cmd.Attempt, cmd.Id, cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &types.Result{
		Kind: types.StoreUpdateNotification,
		UpdateNotification: &types.AlterNotificationsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *SqliteStoreWorker) deleteNotification(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteNotificationCommand) (*types.Result, error) {
	// insert
	res, err := stmt.Exec(cmd.Id, cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &types.Result{
		Kind: types.StoreDeleteNotification,
		DeleteNotification: &types.AlterNotificationsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}
