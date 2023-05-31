package adapter

import (
	"context"
	"database/sql"
	"io"
)

func New(value *sql.DB) Handle { return sqlDB{DB: value} }
func Open(driver, dataSource string) Handle {
	if handle, err := sql.Open(driver, dataSource); err != nil {
		panic(err)
	} else {
		return New(handle)
	}
}

type Handle interface {
	ReadWriter
	BeginTx(ctx context.Context, options *sql.TxOptions) (Transaction, error)
	io.Closer

	DBHandle() *sql.DB
}

type Reader interface {
	QueryContext(ctx context.Context, statement string, args ...any) (QueryResult, error)
	QueryRowContext(ctx context.Context, statement string, args ...any) RowScanner
}
type QueryResult interface {
	RowScanner
	Next() bool
	Err() error
	io.Closer
}
type RowScanner interface {
	Scan(...any) error
}

type Writer interface {
	ExecContext(ctx context.Context, statement string, args ...any) (sql.Result, error)
}
type ReadWriter interface {
	Reader
	Writer
}

type Transaction interface {
	ReadWriter
	Transactional

	TxHandle() *sql.Tx
}
type Transactional interface {
	Commit() error
	Rollback() error
}
