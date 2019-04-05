package meda

import (
	"github.com/go-sql-driver/mysql"
)

// RedactDSN returns dsn with concrete passwords redacted.
// If dsn is not a valid DSN parsable with go-sql-driver/mysql, dsn is simply
// returned. If the password in dsn is empty, dsn is returned as is.
func RedactDSN(dsn string) string {
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		return dsn
	}

	if len(config.Passwd) > 0 {
		config.Passwd = "[REDACTED]"
	}

	return config.FormatDSN()
}
