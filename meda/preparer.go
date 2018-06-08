package meda

import (
	"github.com/jmoiron/sqlx"
)

var _ NamedPreparer = &sqlx.DB{}
var _ NamedPreparer = &sqlx.Tx{}

type NamedPreparer interface {
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
}
