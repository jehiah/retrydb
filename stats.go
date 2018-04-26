package retrydb

import (
	"database/sql"
	"sync/atomic"
	"time"
)

// DBStats contains database statistics.
type DBStats struct {
	Primary      *sql.DBStats `json:"primary"`
	Secondary    *sql.DBStats `json:"secondary"`
	RetryUntil   time.Time    `json:"retry_until"`
	RetryUntilTs int64        `json:"retry_until_ts"`
	RetryCount   int32        `json:"retry_count"`
}
type hasStats interface {
	Stats() sql.DBStats
}

// Stats returns database statistics for the primary and secondary database
func (r *RetryDB) Stats() (d DBStats) {
	if db, ok := r.Primary.(hasStats); ok && r.Primary != nil {
		v := db.Stats()
		d.Primary = &v
	}
	if db, ok := r.Secondary.(hasStats); ok && r.Secondary != nil {
		v := db.Stats()
		d.Secondary = &v
	}
	r.RLock()
	retryUntil := r.retryUntil
	r.RUnlock()
	if time.Now().Before(retryUntil) {
		d.RetryUntil = retryUntil
		d.RetryUntilTs = retryUntil.Unix()
	}
	d.RetryCount = atomic.LoadInt32(&r.retryCount)
	return
}
