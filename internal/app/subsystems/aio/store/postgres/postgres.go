package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/notification"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
	"github.com/resonatehq/resonate/pkg/timeout"

	_ "github.com/lib/pq"
)

const (
	CREATE_TABLE_STATEMENT = `
	CREATE TABLE IF NOT EXISTS promises (
		id            TEXT,
		sort_id       SERIAL,
		state         INTEGER DEFAULT 1,
		param_headers BYTEA,
		param_ikey    TEXT,
		param_data    BYTEA,
		value_headers BYTEA,
		value_ikey    TEXT,
		value_data    BYTEA,
		timeout       BIGINT,
		tags          BYTEA,
		created_on    BIGINT,
		completed_on  BIGINT,
		PRIMARY KEY(id)
	);

	CREATE INDEX IF NOT EXISTS idx_promises_sort_id ON promises(sort_id);

	CREATE TABLE IF NOT EXISTS timeouts (
		id   TEXT,
		time BIGINT,
		PRIMARY KEY(id)
	);

	CREATE TABLE IF NOT EXISTS subscriptions (
		id           TEXT,
		sort_id      SERIAL,
		promise_id   TEXT,
		url          TEXT,
		retry_policy BYTEA,
		created_on   BIGINT,
		PRIMARY KEY(id, promise_id)
	);

	CREATE INDEX IF NOT EXISTS idx_subscriptions_sort_id ON subscriptions(sort_id);

	CREATE TABLE IF NOT EXISTS notifications (
		id           TEXT,
		promise_id   TEXT,
		url          TEXT,
		retry_policy BYTEA,
		time         BIGINT,
		attempt      INTEGER,
		PRIMARY KEY(id, promise_id)
	);`

	DROP_TABLE_STATEMENT = `
	DROP TABLE notifications;
	DROP TABLE subscriptions;
	DROP TABLE timeouts;
	DROP TABLE promises;`

	PROMISE_SELECT_STATEMENT = `
	SELECT
        id, state, param_headers, param_ikey, param_data, value_headers, value_ikey, value_data, timeout, tags, created_on, completed_on
    FROM
        promises
    WHERE
        id = $1`

	PROMISE_SEARCH_STATEMENT = `
	SELECT
		id, state, param_headers, param_ikey, param_data, value_headers, value_ikey, value_data, timeout, tags, created_on, completed_on, sort_id
	FROM
		promises
	WHERE
		($1::int IS NULL OR sort_id < $1) AND
		(state & $2 != 0)
	ORDER BY
		sort_id DESC
	LIMIT
		$3`

	PROMISE_INSERT_STATEMENT = `
	INSERT INTO promises
	    (id, state, param_headers, param_ikey, param_data, timeout, tags, created_on)
	VALUES
	    ($1, $2, $3, $4, $5, $6, $7, $8)
	ON CONFLICT(id) DO NOTHING`

	PROMISE_UPDATE_STATMENT = `
	UPDATE
		promises
    SET
		state = $1, value_headers = $2, value_ikey = $3, value_data = $4, completed_on = $5
    WHERE
		id = $6 AND state = 1`

	PROMISE_UPDATE_TIMEOUT_STATEMENT = `
		UPDATE
			promises
		SET
			state = 8, completed_on = timeout
		WHERE
			state = 1 AND timeout <= $1`

	TIMEOUT_SELECT_STATEMENT = `
	SELECT
        id, time
    FROM
        timeouts
    ORDER BY
        time ASC, id
    LIMIT $1`

	TIMEOUT_INSERT_STATEMENT = `
	INSERT INTO timeouts
        (id, time)
    VALUES
        ($1, $2)
	ON CONFLICT(id) DO NOTHING`

	TIMEOUT_DELETE_STATEMENT = `
	DELETE FROM timeouts WHERE id = $1`

	SUBSCRIPTION_SELECT_STATEMENT = `
	SELECT
		id, promise_id, url, retry_policy, created_on
	FROM
		subscriptions
	WHERE
		id = $1 AND promise_id = $2`

	SUBSCRIPTION_SELECT_ALL_STATEMENT = `
	SELECT
		id, promise_id, url, retry_policy, created_on, sort_id
	FROM
		subscriptions
	WHERE
		($1::int IS NULL OR sort_id < $1) AND
		promise_id = $2
	ORDER BY
		sort_id DESC
	LIMIT
		$3`

	SUBSCRIPTION_INSERT_STATEMENT = `
	INSERT INTO subscriptions
        (id, promise_id, url, retry_policy, created_on)
    VALUES
        ($1, $2, $3, $4, $5)
	ON CONFLICT(id, promise_id) DO NOTHING`

	SUBSCRIPTION_DELETE_STATEMENT = `
	DELETE FROM subscriptions WHERE id = $1 AND promise_id = $2`

	SUBSCRIPTION_DELETE_ALL_STATEMENT = `
	DELETE FROM subscriptions WHERE promise_id = $1`

	SUBSCRIPTION_DELETE_ALL_TIMEOUT_STATEMENT = `
	DELETE FROM
		subscriptions
	WHERE
		promise_id IN (SELECT id FROM promises WHERE state = 1 AND timeout <= $1)`

	NOTIFICATION_SELECT_STATEMENT = `
	SELECT
        id, promise_id, url, retry_policy, time, attempt
    FROM
        notifications
    ORDER BY
		time ASC, promise_id, id
    LIMIT $1`

	NOTIFICATION_INSERT_STATEMENT = `
	INSERT INTO notifications
		(id, promise_id, url, retry_policy, time, attempt)
	SELECT
		id, promise_id, url, retry_policy, $1, 0
	FROM
		subscriptions
	WHERE
		promise_id = $2
	ON CONFLICT(id, promise_id) DO NOTHING`

	NOTIFICATION_INSERT_TIMEOUT_STATEMENT = `
	INSERT INTO notifications
		(id, promise_id, url, retry_policy, time, attempt)
	SELECT
		id, promise_id, url, retry_policy, $1, 0
	FROM
		subscriptions
	WHERE
		promise_id IN (SELECT id FROM promises WHERE state = 1 AND timeout <= $1)
	ON CONFLICT(id, promise_id) DO NOTHING`

	NOTIFICATION_UPDATE_STATEMENT = `
	UPDATE notifications
    SET time = $1, attempt = $2
    WHERE id = $3 AND promise_id = $4`

	NOTIFICATION_DELETE_STATEMENT = `
	DELETE FROM notifications WHERE id = $1 AND promise_id = $2`
)

type Config struct {
	Host      string
	Port      string
	Username  string
	Password  string
	Database  string
	TxTimeout time.Duration
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
	dbUrl := &url.URL{
		User:     url.UserPassword(config.Username, config.Password),
		Host:     fmt.Sprintf("%s:%s", config.Host, config.Port),
		Path:     config.Database,
		Scheme:   "postgres",
		RawQuery: "sslmode=disable",
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

func (w *PostgresStoreWorker) Process(sqes []*bus.SQE[types.Submission, types.Completion]) []*bus.CQE[types.Submission, types.Completion] {
	return store.Process(w, sqes)
}

func (w *PostgresStoreWorker) Execute(transactions []*types.Transaction) ([][]*types.Result, error) {
	util.Assert(len(transactions) > 0, "expected a transaction")

	ctx, cancel := context.WithTimeout(context.Background(), w.config.TxTimeout)
	defer cancel()

	tx, err := w.db.BeginTx(ctx, nil)
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

func (w *PostgresStoreWorker) performCommands(tx *sql.Tx, transactions []*types.Transaction) ([][]*types.Result, error) {
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
			case types.StoreTimeoutPromises:
				util.Assert(command.TimeoutPromises != nil, "command must not be nil")
				results[i][j], err = w.timeoutPromises(tx, promiseUpdateTimeoutStmt, command.TimeoutPromises)

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
			case types.StoreTimeoutDeleteSubscriptions:
				util.Assert(command.TimeoutDeleteSubscriptions != nil, "command must not be nil")
				results[i][j], err = w.timeoutDeleteSubscriptions(tx, subscriptionDeleteAllTimeoutStmt, command.TimeoutDeleteSubscriptions)

			// Notification
			case types.StoreReadNotifications:
				util.Assert(command.ReadNotifications != nil, "command must not be nil")
				results[i][j], err = w.readNotifications(tx, command.ReadNotifications)
			case types.StoreCreateNotifications:
				util.Assert(command.CreateNotifications != nil, "command must not be nil")
				results[i][j], err = w.createNotifications(tx, notificationInsertStmt, command.CreateNotifications)
			case types.StoreUpdateNotification:
				util.Assert(command.UpdateNotification != nil, "command must not be nil")
				results[i][j], err = w.updateNotification(tx, notificationUpdateStmt, command.UpdateNotification)
			case types.StoreDeleteNotification:
				util.Assert(command.DeleteNotification != nil, "command must not be nil")
				results[i][j], err = w.deleteNotification(tx, notificationDeleteStmt, command.DeleteNotification)
			case types.StoreTimeoutCreateNotifications:
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

func (w *PostgresStoreWorker) readPromise(tx *sql.Tx, cmd *types.ReadPromiseCommand) (*types.Result, error) {
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

func (w *PostgresStoreWorker) searchPromises(tx *sql.Tx, cmd *types.SearchPromisesCommand) (*types.Result, error) {
	// convert list of state to bit mask
	mask := 0
	for _, state := range cmd.States {
		mask = mask | int(state)
	}

	// select
	rows, err := tx.Query(PROMISE_SEARCH_STATEMENT, cmd.SortId, mask, cmd.Limit)
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
			&record.ParamIkey,
			&record.ParamData,
			&record.ValueHeaders,
			&record.ValueIkey,
			&record.ValueData,
			&record.Timeout,
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

	return &types.Result{
		Kind: types.StoreSearchPromises,
		SearchPromises: &types.QueryPromisesResult{
			RowsReturned: rowsReturned,
			LastSortId:   lastSortId,
			Records:      records,
		},
	}, nil
}

func (w *PostgresStoreWorker) createPromise(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreatePromiseCommand) (*types.Result, error) {
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

func (w *PostgresStoreWorker) updatePromise(tx *sql.Tx, stmt *sql.Stmt, cmd *types.UpdatePromiseCommand) (*types.Result, error) {
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

func (w *PostgresStoreWorker) timeoutPromises(tx *sql.Tx, stmt *sql.Stmt, cmd *types.TimeoutPromisesCommand) (*types.Result, error) {
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

	return &types.Result{
		Kind: types.StoreTimeoutPromises,
		TimeoutPromises: &types.AlterPromisesResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) readTimeouts(tx *sql.Tx, cmd *types.ReadTimeoutsCommand) (*types.Result, error) {
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

func (w *PostgresStoreWorker) createTimeout(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreateTimeoutCommand) (*types.Result, error) {
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

func (w *PostgresStoreWorker) deleteTimeout(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteTimeoutCommand) (*types.Result, error) {
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

func (w *PostgresStoreWorker) readSubscription(tx *sql.Tx, cmd *types.ReadSubscriptionCommand) (*types.Result, error) {
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

func (w *PostgresStoreWorker) readSubscriptions(tx *sql.Tx, cmd *types.ReadSubscriptionsCommand) (*types.Result, error) {
	// select
	rows, err := tx.Query(SUBSCRIPTION_SELECT_ALL_STATEMENT, cmd.SortId, cmd.PromiseId, cmd.Limit)
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

	return &types.Result{
		Kind: types.StoreReadSubscriptions,
		ReadSubscriptions: &types.QuerySubscriptionsResult{
			RowsReturned: rowsReturned,
			LastSortId:   lastSortId,
			Records:      records,
		},
	}, nil
}

func (w *PostgresStoreWorker) createSubscription(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreateSubscriptionCommand) (*types.Result, error) {
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
		CreateSubscription: &types.AlterSubscriptionsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) deleteSubscription(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteSubscriptionCommand) (*types.Result, error) {
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
		DeleteSubscription: &types.AlterSubscriptionsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) deleteSubscriptions(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteSubscriptionsCommand) (*types.Result, error) {
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
		DeleteSubscriptions: &types.AlterSubscriptionsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) timeoutDeleteSubscriptions(tx *sql.Tx, stmt *sql.Stmt, cmd *types.TimeoutDeleteSubscriptionsCommand) (*types.Result, error) {
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

	return &types.Result{
		Kind: types.StoreTimeoutDeleteSubscriptions,
		TimeoutDeleteSubscriptions: &types.AlterSubscriptionsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) readNotifications(tx *sql.Tx, cmd *types.ReadNotificationsCommand) (*types.Result, error) {
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

func (w *PostgresStoreWorker) createNotifications(tx *sql.Tx, stmt *sql.Stmt, cmd *types.CreateNotificationsCommand) (*types.Result, error) {
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

	return &types.Result{
		Kind: types.StoreCreateNotifications,
		CreateNotifications: &types.AlterNotificationsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

func (w *PostgresStoreWorker) updateNotification(tx *sql.Tx, stmt *sql.Stmt, cmd *types.UpdateNotificationCommand) (*types.Result, error) {
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

func (w *PostgresStoreWorker) deleteNotification(tx *sql.Tx, stmt *sql.Stmt, cmd *types.DeleteNotificationCommand) (*types.Result, error) {
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

func (w *PostgresStoreWorker) timeoutCreateNotifications(tx *sql.Tx, stmt *sql.Stmt, cmd *types.TimeoutCreateNotificationsCommand) (*types.Result, error) {
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

	return &types.Result{
		Kind: types.StoreTimeoutCreateNotifications,
		TimeoutCreateNotifications: &types.AlterNotificationsResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}
