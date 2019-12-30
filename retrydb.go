package retrydb

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// RetryDB is a wrapper around multiple *sql.DB objects providing transparent retry of queries against the secondary.
type RetryDB struct {
	Primary          Retryable
	Secondary        Retryable
	retryCount       uint32
	retryUntil       time.Time
	maxQueryTime     time.Duration
	retryStrategy    RetryStrategy
	secondaryQueries uint32
	sync.RWMutex
}

type RetryStrategy func(uint32) time.Duration

func defaultRetryStrategy(retryCount uint32) time.Duration {
	if retryCount < 4 {
		return time.Duration(retryCount) * 30 * time.Second
	}
	return 120 * time.Second
}

// Open connections to the Primary and Secondary Database
func Open(primaryDriverName, primaryDataSourceName, secondaryDriverName, secondaryDataSourceName string) (*RetryDB, error) {
	p, err := sql.Open(primaryDriverName, primaryDataSourceName)
	if err != nil {
		return nil, err
	}
	db := &RetryDB{
		Primary:       p,
		maxQueryTime:  30 * time.Second,
		retryStrategy: defaultRetryStrategy,
	}
	if secondaryDataSourceName != "" {
		s, err := sql.Open(secondaryDriverName, secondaryDataSourceName)
		if err != nil {
			p.Close()
			return nil, err
		}
		db.Secondary = s
	}
	return db, nil
}

func OpenWithDB(primary *sql.DB, secondary *sql.DB) *RetryDB {
	return &RetryDB{
		Primary:       primary,
		Secondary:     secondary,
		maxQueryTime:  30 * time.Second,
		retryStrategy: defaultRetryStrategy,
	}
}

func (db *RetryDB) SetMaxQueryTime(t time.Duration) {
	db.Lock()
	db.maxQueryTime = t
	db.Unlock()
}

// Set the retry interval for which the master will be retried after a query failure. (all queries go to secondary)
func (db *RetryDB) SetRetryStrategy(s RetryStrategy) {
	db.Lock()
	db.retryStrategy = s
	db.Unlock()
}

func (r *RetryDB) SetMaxOpenConns(n int) {
	if db, ok := r.Primary.(*sql.DB); ok {
		db.SetMaxOpenConns(n)
	}
	if r.Secondary != nil {
		if db, ok := r.Secondary.(*sql.DB); ok {
			db.SetMaxOpenConns(n)
		}
	}
}

// SetMaxIdleConns propagates to the Primary and Secondary database connections
func (r *RetryDB) SetMaxIdleConns(n int) {
	if db, ok := r.Primary.(*sql.DB); ok {
		db.SetMaxOpenConns(n)
	}
	if r.Secondary != nil {
		if db, ok := r.Secondary.(*sql.DB); ok {
			db.SetMaxIdleConns(n)
		}
	}
}

// Transaction against Primary
func (db *RetryDB) Begin() (*sql.Tx, error) { return db.Primary.Begin() }

// Exec against Primary
func (db *RetryDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.Primary.Exec(query, args...)
}

// Driver of Primary
func (db *RetryDB) Driver() driver.Driver { return db.Primary.Driver() }

// returns error if both Primary and Secondary fail pings
func (db *RetryDB) Ping() error {
	err := db.Primary.Ping()
	if err != nil && db.Secondary == nil {
		return err
	}
	if db.Secondary != nil {
		return db.Secondary.Ping()
	}
	return err
}

func (db *RetryDB) Prepare(query string) (*sql.Stmt, error) {
	panic("not implemented")
}

func getFatalError(a error, r *sql.Rows) error {
	if a != nil && a != sql.ErrNoRows {
		return a
	}
	if r == nil {
		return nil
	}
	if r.Err() != nil && r.Err() != sql.ErrNoRows {
		return r.Err()
	}
	return nil
}

func (db *RetryDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if db.Secondary == nil {
		return db.Primary.Query(query, args...)
	}

	// if already in retry; just query the Secondary
	db.RLock()
	until := db.retryUntil
	db.RUnlock()
	start := time.Now()
	if start.Before(until) {
		atomic.AddUint32(&db.secondaryQueries, 1)
		return db.Secondary.Query(query, args...)
	}

	rows, err := db.Primary.Query(query, args...)
	// it's important to peek into Err here
	if ferr := getFatalError(err, rows); ferr != nil {
		db.updateRetry(fmt.Errorf("query errored with %s. retrying against secondary. sql:%q", ferr, query))
		rows, err = db.Secondary.Query(query, args...)
	} else {
		// query succeeded
		queryDuration := time.Since(start)
		db.RLock()
		tooLong := queryDuration > db.maxQueryTime
		db.RUnlock()
		if tooLong {
			// but it took too long
			db.updateRetry(fmt.Errorf("query exceeded allowed limit (%s). sql:%q", queryDuration, query))
		} else if atomic.LoadUint32(&db.retryCount) > 0 {
			db.updateRetry(nil)
		}
	}
	return rows, err
}

func (db *RetryDB) updateRetry(err error) {
	db.Lock()
	if err == nil {
		db.retryUntil = time.Now()
		atomic.StoreUint32(&db.retryCount, 0)
		atomic.StoreUint32(&db.secondaryQueries, 0)
		log.Printf("re-enabling master. %d queries run against secondary", atomic.LoadUint32(&db.secondaryQueries))
	} else {
		until := time.Now().Add(db.retryStrategy(db.retryCount))
		if db.retryCount == 0 {
			log.Printf("disabling master until %s. %s", until, err)
		} else {
			log.Printf("updating master disabled until %s. %s", until, err)
		}
		db.retryCount += 1
		db.retryUntil = until
	}
	db.Unlock()
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (db *RetryDB) QueryRow(query string, args ...interface{}) *Row {
	rows, err := db.Query(query, args...)
	return &Row{rows: rows, err: err}
}

func (db *RetryDB) Close() error {
	err := db.Primary.Close()
	var sErr error
	if db.Secondary != nil {
		sErr = db.Secondary.Close()
	}
	if err != nil {
		return err
	}
	return sErr
}

// Row is the result of calling QueryRow to select a single row.
type Row struct {
	// One of these two will be non-nil:
	err  error // deferred error for easy chaining
	rows *sql.Rows
}

// Scan copies the columns from the matched row into the values
// pointed at by dest.  If more than one row matches the query,
// Scan uses the first row and discards the rest.  If no row matches
// the query, Scan returns ErrNoRows.
func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	// TODO(bradfitz): for now we need to defensively clone all
	// []byte that the driver returned (not permitting
	// *RawBytes in Rows.Scan), since we're about to close
	// the Rows in our defer, when we return from this function.
	// the contract with the driver.Next(...) interface is that it
	// can return slices into read-only temporary memory that's
	// only valid until the next Scan/Close.  But the TODO is that
	// for a lot of drivers, this copy will be unnecessary.  We
	// should provide an optional interface for drivers to
	// implement to say, "don't worry, the []bytes that I return
	// from Next will not be modified again." (for instance, if
	// they were obtained from the network anyway) But for now we
	// don't care.
	defer r.rows.Close()
	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := r.rows.Scan(dest...)
	if err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	if err := r.rows.Close(); err != nil {
		return err
	}

	return nil
}

// Retryable is the Interface for something RetryDB can retry
type Retryable interface {
	Begin() (*sql.Tx, error)
	Close() error
	Driver() driver.Driver
	Exec(query string, args ...interface{}) (sql.Result, error)
	Ping() error
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}
