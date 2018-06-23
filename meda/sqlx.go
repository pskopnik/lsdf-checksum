package meda

import (
	"context"

	"github.com/jmoiron/sqlx"
)

var _ NamedPreparer = &sqlx.DB{}
var _ NamedPreparer = &sqlx.Tx{}

type NamedPreparer interface {
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
}

var _ NamedPreparerContext = &sqlx.DB{}
var _ NamedPreparerContext = &sqlx.Tx{}

type NamedPreparerContext interface {
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
}

var _ RebindQueryerContext = &sqlx.DB{}
var _ RebindQueryerContext = &sqlx.Tx{}

type RebindQueryerContext interface {
	sqlx.QueryerContext

	Rebind(query string) string
}
