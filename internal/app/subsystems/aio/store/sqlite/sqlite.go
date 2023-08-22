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

	"github.com/mattn/go-sqlite3"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/notification"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
	"github.com/resonatehq/resonate/pkg/timeout"
)

const (
	CREATE_TABLE_STATEMENT = `
	CREATE TABLE IF NOT EXISTS "Promises" (
		"id"           TEXT,
		"state"        INTEGER DEFAULT 0,
		"paramHeaders" BLOB,
		"paramIkey"    TEXT,
		"paramData"    BLOB,
		"valueHeaders" BLOB,
		"valueIkey"    TEXT,
		"valueData"    BLOB,
		"timeout"      INTEGER,
		PRIMARY KEY("id")
	);
	 CREATE TABLE IF NOT EXISTS "Timeouts" (
		"id"   TEXT,
		"time" INTEGER,
		PRIMARY KEY("id")
	);
	CREATE TABLE IF NOT EXISTS "Subscriptions" (
		"id"          INTEGER PRIMARY KEY AUTOINCREMENT,
		"promiseId"   TEXT,
		"url"         TEXT,
		"retryPolicy" BLOB,
		UNIQUE("promiseId", "url"),
		FOREIGN KEY("promiseId") REFERENCES Promises("id")
	);
	CREATE TABLE IF NOT EXISTS "Notifications" (
		"id"          INTEGER PRIMARY KEY AUTOINCREMENT,
		"promiseId"   TEXT,
		"url"         TEXT,
		"retryPolicy" BLOB,
		"time"        INTEGER,
		"attempt"     INTEGER,
		UNIQUE("promiseId", "url"),
		FOREIGN KEY("promiseId") REFERENCES Promises("id")
	);`

	PROMISE_SELECT_STATEMENT = `
	SELECT
		id, state, paramHeaders, paramIkey, paramData, valueHeaders, valueIkey, valueData, timeout
	FROM
		Promises
	WHERE
		id = ?`

	PROMISE_SEARCH_STATEMENT = `
	SELECT
		id, state, paramHeaders, paramIkey, paramData, valueHeaders, valueIkey, valueData, timeout
	FROM
		Promises
	WHERE
		id GLOB ? AND state = ?`

	PROMISE_INSERT_STATEMENT = `
	INSERT INTO Promises
		(id, state, paramHeaders, paramIkey, paramData, timeout)
	VALUES
		(?, ?, ?, ?, ?, ?)`

	PROMISE_UPDATE_STATMENT = `
	UPDATE Promises
	SET state = ?, valueHeaders = ?, valueIkey = ?, valueData = ?
	WHERE id = ? AND state = 1`

	TIMEOUT_SELECT_STATEMENT = `
	SELECT
		id, time
	FROM
		Timeouts
	ORDER BY
		time ASC
	LIMIT ?`

	TIMEOUT_INSERT_STATEMENT = `
	INSERT INTO Timeouts
		(id, time)
	VALUES
		(?, ?)`

	TIMEOUT_DELETE_STATEMENT = `
	DELETE FROM Timeouts WHERE id = ?`

	SUBSCRIPTION_SELECT_STATEMENT = `
	SELECT
		id, promiseId, url, retryPolicy
	FROM
		Subscriptions
	WHERE
		promiseId IN (%s)`

	SUBSCRIPTION_INSERT_STATEMENT = `
	INSERT INTO Subscriptions
		(promiseId, url, retryPolicy)
	VALUES
		(?, ?, ?)`

	SUBSCRIPTION_DELETE_STATEMENT = `
	DELETE FROM Subscriptions WHERE id = ? AND promiseId = ?`

	NOTIFICATION_SELECT_STATEMENT = `
	SELECT
		id, promiseId, url, retryPolicy, time, attempt
	FROM
		Notifications
	ORDER BY
		time ASC
	LIMIT ?`

	NOTIFICATION_INSERT_STATEMENT = `
	INSERT INTO Notifications
		(promiseId, url, retryPolicy, time, attempt)
	VALUES
		(?, ?, ?, ?, 0)`

	NOTIFICATION_UPDATE_STATEMENT = `
	UPDATE Notifications
	SET time = ?, attempt = ?
	WHERE id = ?`

	NOTIFICATION_DELETE_STATEMENT = `
	DELETE FROM Notifications WHERE id = ?`
)

type Config struct {
	Path string
}

type SqliteStore struct {
	db     *sql.DB
	config Config
}

type SqliteStoreDevice struct {
	*SqliteStore
}

func New(config Config) (aio.Subsystem, error) {
	db, err := sql.Open("sqlite3", config.Path)
	if err != nil {
		return nil, err
	}

	return &SqliteStore{
		db:     db,
		config: config,
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
	return os.Remove(s.config.Path)
}

func (s *SqliteStore) NewWorker(int) aio.Worker {
	return &SqliteStoreDevice{s}
}

func (d *SqliteStoreDevice) Process(sqes []*bus.SQE[types.Submission, types.Completion]) []*bus.CQE[types.Submission, types.Completion] {
	return store.Process(d, sqes)
}

func (d *SqliteStoreDevice) Execute(transactions []*types.Transaction) ([][]*types.Result, error) {
	util.Assert(len(transactions) > 0, "expected a transaction")

	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}

	results, err := d.performCommands(tx, transactions)
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

func (d *SqliteStoreDevice) performCommands(tx *sql.Tx, transactions []*types.Transaction) ([][]*types.Result, error) {
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
				results[i][j], err = d.readPromise(tx, command.ReadPromise)
			case types.StoreSearchPromises:
				util.Assert(command.SearchPromises != nil, "command must not be nil")
				results[i][j], err = d.searchPromises(tx, command.SearchPromises)
			case types.StoreCreatePromise:
				util.Assert(command.CreatePromise != nil, "command must not be nil")
				results[i][j], err = d.createPromise(tx, promiseInsertStmt, command.CreatePromise)
			case types.StoreUpdatePromise:
				util.Assert(command.UpdatePromise != nil, "command must not be nil")
				results[i][j], err = d.updatePromise(tx, promiseUpdateStmt, command.UpdatePromise)

			// Timeout
			case types.StoreReadTimeouts:
				util.Assert(command.ReadTimeouts != nil, "command must not be nil")
				results[i][j], err = d.readTimeouts(tx, command.ReadTimeouts)
			case types.StoreCreateTimeout:
				util.Assert(command.CreateTimeout != nil, "command must not be nil")
				results[i][j], err = d.createTimeout(tx, timeoutInsertStmt, command.CreateTimeout)
			case types.StoreDeleteTimeout:
				util.Assert(command.DeleteTimeout != nil, "command must not be nil")
				results[i][j], err = d.deleteTimeout(tx, timeoutDeleteStmt, command.DeleteTimeout)

			// Subscription
			case types.StoreReadSubscriptions:
				util.Assert(command.ReadSubscriptions != nil, "command must not be nil")
				results[i][j], err = d.readSubscriptions(tx, command.ReadSubscriptions)
			case types.StoreCreateSubscription:
				util.Assert(command.CreateSubscription != nil, "command must not be nil")
				results[i][j], err = d.createSubscription(tx, subscriptionInsertStmt, command.CreateSubscription)
			case types.StoreDeleteSubscription:
				util.Assert(command.DeleteSubscription != nil, "command must not be nil")
				results[i][j], err = d.deleteSubscription(tx, subscriptionDeleteStmt, command.DeleteSubscription)

			// Notification
			case types.StoreReadNotifications:
				util.Assert(command.ReadNotifications != nil, "command must not be nil")
				results[i][j], err = d.readNotifications(tx, command.ReadNotifications)
			case types.StoreCreateNotification:
				util.Assert(command.CreateNotification != nil, "command must not be nil")
				results[i][j], err = d.createNotification(tx, notificationInsertStmt, command.CreateNotification)
			case types.StoreUpdateNotification:
				util.Assert(command.UpdateNotification != nil, "command must not be nil")
				results[i][j], err = d.updateNotification(tx, notificationUpdateStmt, command.UpdateNotification)
			case types.StoreDeleteNotification:
				util.Assert(command.DeleteNotification != nil, "command must not be nil")
				results[i][j], err = d.deleteNotification(tx, notificationDeleteStmt, command.DeleteNotification)
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

func (d *SqliteStoreDevice) readPromise(tx *sql.Tx, cmd *types.ReadPromiseCommand) (*types.Result, error) {
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

func (d *SqliteStoreDevice) searchPromises(tx *sql.Tx, cmd *types.SearchPromisesCommand) (*types.Result, error) {
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

func (d *SqliteStoreDevice) createPromise(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreatePromiseCommand) (*types.Result, error) {
	util.Assert(cmd.Param.Headers != nil, "headers must not be nil")

	headers, err := json.Marshal(cmd.Param.Headers)
	if err != nil {
		return nil, err
	}

	// insert
	res, err := stmt.Exec(cmd.Id, promise.Pending, headers, cmd.Param.Ikey, cmd.Param.Data, cmd.Timeout)
	var rowsAffected int64

	if err != nil {
		sqliteErr, ok := err.(sqlite3.Error)
		if !ok || sqliteErr.ExtendedCode != sqlite3.ErrConstraintPrimaryKey {
			return nil, err
		}
	} else {
		rowsAffected, err = res.RowsAffected()
		if err != nil {
			return nil, err
		}
	}

	return &types.Result{
		Kind: types.StoreCreatePromise,
		CreatePromise: &types.AlterPromisesResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (d *SqliteStoreDevice) updatePromise(tx *sql.Tx, stmt *sql.Stmt, cmd *types.UpdatePromiseCommand) (*types.Result, error) {
	util.Assert(cmd.State.In(promise.Resolved|promise.Rejected|promise.Canceled|promise.Timedout), "state must be canceled, resolved, rejected, or timedout")
	util.Assert(cmd.Value.Headers != nil, "headers must not be nil")

	headers, err := json.Marshal(cmd.Value.Headers)
	if err != nil {
		return nil, err
	}

	// update
	res, err := stmt.Exec(cmd.State, headers, cmd.Value.Ikey, cmd.Value.Data, cmd.Id)
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

func (d *SqliteStoreDevice) readTimeouts(tx *sql.Tx, cmd *types.ReadTimeoutsCommand) (*types.Result, error) {
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

func (d *SqliteStoreDevice) createTimeout(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreateTimeoutCommand) (*types.Result, error) {
	util.Assert(cmd.Time >= 0, "time must be non-negative")
	var rowsAffected int64

	// insert
	res, err := stmt.Exec(cmd.Id, cmd.Time)
	if err != nil {
		sqliteErr, ok := err.(sqlite3.Error)
		if !ok || sqliteErr.ExtendedCode != sqlite3.ErrConstraintPrimaryKey {
			return nil, err
		}
	} else {
		rowsAffected, err = res.RowsAffected()
		if err != nil {
			return nil, err
		}
	}

	return &types.Result{
		Kind: types.StoreCreateTimeout,
		CreateTimeout: &types.AlterTimeoutsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (d *SqliteStoreDevice) deleteTimeout(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteTimeoutCommand) (*types.Result, error) {
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

func (d *SqliteStoreDevice) readSubscriptions(tx *sql.Tx, cmd *types.ReadSubscriptionsCommand) (*types.Result, error) {
	util.Assert(len(cmd.PromiseIds) > 0, "expected a promise id")

	// select
	var placeholders string
	promiseIds := make([]interface{}, len(cmd.PromiseIds))

	for i, promiseId := range cmd.PromiseIds {
		if i == len(cmd.PromiseIds)-1 {
			placeholders = placeholders + "?"
		} else {
			placeholders = placeholders + "?,"
		}

		promiseIds[i] = promiseId
	}

	stmt := fmt.Sprintf(SUBSCRIPTION_SELECT_STATEMENT, placeholders)
	rows, err := tx.Query(stmt, promiseIds...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rowsReturned := int64(0)
	var records []*subscription.SubscriptionRecord

	for rows.Next() {
		record := &subscription.SubscriptionRecord{}
		if err := rows.Scan(&record.Id, &record.PromiseId, &record.Url, &record.RetryPolicy); err != nil {
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

func (d *SqliteStoreDevice) createSubscription(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreateSubscriptionCommand) (*types.Result, error) {
	util.Assert(cmd.RetryPolicy != nil, "retry policy must not be nil")

	retryPolicy, err := json.Marshal(cmd.RetryPolicy)
	if err != nil {
		return nil, err
	}

	var rowsAffected int64
	var lastInsertId int64

	// insert
	res, err := stmt.Exec(cmd.PromiseId, cmd.Url, retryPolicy)
	if err != nil {
		sqliteErr, ok := err.(sqlite3.Error)
		if !ok || sqliteErr.ExtendedCode != sqlite3.ErrConstraintUnique {
			return nil, err
		}
	} else {
		rowsAffected, err = res.RowsAffected()
		if err != nil {
			return nil, err
		}

		lastInsertId, err = res.LastInsertId()
		if err != nil {
			return nil, err
		}
	}

	return &types.Result{
		Kind: types.StoreCreateSubscription,
		CreateSubscription: &types.AlterSubscriptionResult{
			RowsAffected: rowsAffected,
			LastInsertId: lastInsertId,
		},
	}, nil
}

func (d *SqliteStoreDevice) deleteSubscription(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteSubscriptionCommand) (*types.Result, error) {
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
		Kind: types.StoreDeleteSubscription,
		DeleteSubscription: &types.AlterSubscriptionResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (d *SqliteStoreDevice) readNotifications(tx *sql.Tx, cmd *types.ReadNotificationsCommand) (*types.Result, error) {
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

func (d *SqliteStoreDevice) createNotification(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreateNotificationCommand) (*types.Result, error) {
	util.Assert(cmd.Time >= 0, "time must be non-negative")
	util.Assert(cmd.RetryPolicy != nil, "retry policy must not be nil")

	var rowsAffected int64
	var lastInsertId int64

	// insert
	res, err := stmt.Exec(cmd.PromiseId, cmd.Url, cmd.RetryPolicy, cmd.Time)
	if err != nil {
		sqliteErr, ok := err.(sqlite3.Error)
		if !ok || sqliteErr.ExtendedCode != sqlite3.ErrConstraintUnique {
			return nil, err
		}
	} else {
		rowsAffected, err = res.RowsAffected()
		if err != nil {
			return nil, err
		}

		lastInsertId, err = res.LastInsertId()
		if err != nil {
			return nil, err
		}
	}

	return &types.Result{
		Kind: types.StoreCreateNotification,
		CreateNotification: &types.AlterNotificationsResult{
			RowsAffected: rowsAffected,
			LastInsertId: lastInsertId,
		},
	}, nil
}

func (d *SqliteStoreDevice) updateNotification(tx *sql.Tx, stmt *sql.Stmt, cmd *types.UpdateNotificationCommand) (*types.Result, error) {
	// update
	res, err := stmt.Exec(cmd.Time, cmd.Attempt, cmd.Id)
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

func (d *SqliteStoreDevice) deleteNotification(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteNotificationCommand) (*types.Result, error) {
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
		Kind: types.StoreDeleteNotification,
		DeleteNotification: &types.AlterNotificationsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}
