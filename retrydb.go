package retrydb

import (
	"database/sql"
	"database/sql/driver"
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
func (db *RetryDB) QueryRow(query string, args ...interface{}) *sql.Row {
	panic("not implemented")
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
