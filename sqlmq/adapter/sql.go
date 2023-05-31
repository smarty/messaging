package adapter

import (
	"context"
	"database/sql"
)

type sqlDB struct{ *sql.DB }

func (this sqlDB) QueryContext(ctx context.Context, statement string, args ...any) (QueryResult, error) {
	return this.DB.QueryContext(ctx, statement, args...)
}
func (this sqlDB) QueryRowContext(ctx context.Context, statement string, args ...any) RowScanner {
	return this.DB.QueryRowContext(ctx, statement, args...)
}
func (this sqlDB) BeginTx(ctx context.Context, options *sql.TxOptions) (Transaction, error) {
	if tx, err := this.DB.BeginTx(ctx, options); err != nil {
		return nil, err
	} else {
		return sqlTx{Tx: tx}, nil
	}
}

func (this sqlDB) DBHandle() *sql.DB { return this.DB }

type sqlTx struct{ *sql.Tx }

func (this sqlTx) QueryContext(ctx context.Context, statement string, args ...any) (QueryResult, error) {
	return this.Tx.QueryContext(ctx, statement, args...)
}
func (this sqlTx) QueryRowContext(ctx context.Context, statement string, args ...any) RowScanner {
	return this.Tx.QueryRowContext(ctx, statement, args...)
}

func (this sqlTx) TxHandle() *sql.Tx { return this.Tx }
