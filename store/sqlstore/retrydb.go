package sqlstore

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

type RetryDB struct {
	*sql.DB
}

func (db *RetryDB) Exec(query string, args ...any) (res sql.Result, err error) {
	for i := 0; i < 10; i++ {
		res, err = db.ExecContext(context.Background(), query, args...)
		if err == nil || strings.Contains(err.Error(), "constraint") {
			return
		}
		time.Sleep(time.Millisecond * 500)
	}
	return
}
