package main

import (
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/jmoiron/sqlx"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

const (
	createTableQuery = "CREATE TABLE `inserts` (" +
		"`id` int(11) NOT NULL AUTO_INCREMENT," +
		"`path` varchar(2048) NOT NULL," +
		"`modification_time` datetime(6) NOT NULL," +
		"`file_size` int(11) NOT NULL," +
		"`last_seen` int(11) NOT NULL," +
		"PRIMARY KEY (`id`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;"

	dropTableQuery = "DROP TABLE `inserts`;"
)

type BulkInserter struct {
	transactionSize int
	db              *sqlx.DB
}

func NewBulkInserter(transactionSize int, db *sqlx.DB) *BulkInserter {
	return &BulkInserter{
		transactionSize: transactionSize,
		db:              db,
	}
}

func (b *BulkInserter) Before() error {
	_, err := b.db.Exec(createTableQuery)
	return err
}

func (b *BulkInserter) After() error {
	_, err := b.db.Exec(dropTableQuery)
	return err
}

func (b *BulkInserter) Run(n int) error {
	var medaInsert meda.Insert
	var count, txCount int

	var (
		path       = "/some/arbitrary/path"
		fileSize   = 1
		modTime    = time.Now()
		modTimeInc = time.Second
		lastSeen   = 1
	)

	tx, prepStmt, err := b.openWriteInsertsTx()
	if err != nil {
		return err
	}
	defer prepStmt.Close()
	defer tx.Rollback()

	for count < n {
		fileSize += 1
		modTime = modTime.Add(modTimeInc)

		medaInsert = meda.Insert{
			Path:             path,
			ModificationTime: modTime,
			FileSize:         fileSize,
			LastSeen:         lastSeen,
		}

		_, err = prepStmt.Exec(&medaInsert)
		if err != nil {
			return err
		}

		count += 1
		txCount += 1

		if txCount >= b.transactionSize {
			err = b.closeWriteInsertsTx(tx, prepStmt)
			if err != nil {
				return err
			}

			tx, prepStmt, err = b.openWriteInsertsTx()
			if err != nil {
				return err
			}

			txCount = 0
		}
	}

	err = b.closeWriteInsertsTx(tx, prepStmt)
	if err != nil {
		return err
	}

	return nil
}

func (b *BulkInserter) openWriteInsertsTx() (*sqlx.Tx, *sqlx.NamedStmt, error) {
	tx, err := b.db.Beginx()
	if err != nil {
		return nil, nil, err
	}

	prepStmt, err := meda.InsertsPrepareInsert(tx)
	if err != nil {
		_ = tx.Rollback()
		return nil, nil, err
	}

	return tx, prepStmt, nil
}

func (b *BulkInserter) closeWriteInsertsTx(tx *sqlx.Tx, prepStmt *sqlx.NamedStmt) error {
	err := prepStmt.Close()
	if err != nil {
		// Try to commit
		_ = tx.Commit()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func main() {
	db, err := sqlx.Open("mysql", "root:@/bench_schema")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var insertsNum = 1000000

	transactionSizes := []int{
		1,
		10,
		100,
		1000,
		5000,
		10000,
		20000,
		50000,
		100000,
	}

	for i := 0; i < 10; i++ {
		for _, transactionSize := range transactionSizes {
			duration, err := runRound(db, insertsNum, transactionSize)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Println(insertsNum, transactionSize, duration.Seconds())
		}
	}
}

func runRound(db *sqlx.DB, insertsNum, transactionSize int) (time.Duration, error) {
	bulkInserter := NewBulkInserter(transactionSize, db)

	err := bulkInserter.Before()
	if err != nil {
		return time.Duration(0), err
	}

	start := time.Now()

	err = bulkInserter.Run(insertsNum)
	if err != nil {
		return time.Duration(0), err
	}

	end := time.Now()

	err = bulkInserter.After()
	if err != nil {
		return time.Duration(0), err
	}

	return end.Sub(start), nil
}
