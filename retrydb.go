package retrydb

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"log"
)

// a wrapper around multiple *sql.DB objects providing transparent retry of queries against the slave.

type RetryDB struct {
	Primary   *sql.DB
	Secondary *sql.DB
}

// Open connections to the Primary and Secondary Database
func Open(primaryDriverName, primaryDataSourceName, secondaryDriverName, secondaryDataSourceName string) (*RetryDB, error) {
	p, err := sql.Open(primaryDriverName, primaryDataSourceName)
	if err != nil {
		return nil, err
	}
	var s *sql.DB
	if secondaryDataSourceName != "" {
		s, err = sql.Open(secondaryDriverName, secondaryDataSourceName)
		if err != nil {
			p.Close()
			return nil, err
		}
	}
	db := &RetryDB{p, s}
	return db, nil
}

func (db *RetryDB) Begin() (*sql.Tx, error) { return db.Primary.Begin() }
func (db *RetryDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.Primary.Exec(query, args...)
}
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
func (db *RetryDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Primary.Query(query, args...)
	// it's important to peek into Err here
	if (err != nil || rows.Err() != nil) && db.Secondary != nil {
		log.Printf("retrying against secondary err: %s", err)
		rows, err = db.Secondary.Query(query, args...)
	}
	return rows, err
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
