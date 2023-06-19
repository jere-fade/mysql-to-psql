package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"
)

const (
	mysql_serverID = 1
	flavor         = "mysql"
	mysql_host     = "127.0.0.1"
	mysql_port     = 3306

	pq_host      = "127.0.0.1"
	pq_port      = 5432
	pq_defaultDB = "postgres"
)

var mysql_user = os.Getenv("mysql_user")
var mysql_password = os.Getenv("mysql_password")
var pq_user = os.Getenv("pq_user")
var pq_password = os.Getenv("pq_password")

func main() {

	if mysql_user == "" {
		panic("mysql_user not set")
	}
	if mysql_password == "" {
		panic("mysql_password not set")
	}
	if pq_user == "" {
		panic("pq_user not set")
	}
	if pq_password == "" {
		panic("pq_password not set")
	}

	fmt.Printf("[MYSQL CONNECTION]       %s@%s:%d\n", mysql_user, mysql_host, mysql_port)
	fmt.Printf("[POSTGRESQL CONNECTION]  %s@%s:%d\n", pq_user, pq_host, pq_port)

	pos, err := readPos()
	check(err)

	syncer := &Syncer{
		Position: *pos,
		Host:     mysql_host,
		Port:     mysql_port,
		User:     mysql_user,
		Password: mysql_password,
		tables:   make(map[uint64]*TableMapEvent),
		count:    0,
	}

	start := time.Now()

	err = syncer.syncLog()
	check(err)

	// psql connection
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		pq_host,
		pq_port,
		pq_user,
		pq_password,
		pq_defaultDB,
	)

	psql := &PsqlConn{pq_dbname: pq_defaultDB, recordCount: 0}
	psql.conn, err = sql.Open("postgres", psqlconn)
	check(err)
	defer psql.conn.Close()

	for {
		ev, err := syncer.getEvent()

		if err != nil {
			check(err)
		} else if ev == nil {
			break
		}

		switch ev.EventHeader.EventType {

		case QUERY_EVENT:
			if qe, ok := ev.Event.(*QueryEvent); ok {
				err = psql.processQuery(qe)
				check(err)
			}
		case WRITE_ROWS_EVENTv0,
			WRITE_ROWS_EVENTv1,
			WRITE_ROWS_EVENTv2:
			if re, ok := ev.Event.(*RowsEvent); ok {
				err = psql.processWriteRow(re)
				check(err)
			}
		case DELETE_ROWS_EVENTv0,
			DELETE_ROWS_EVENTv1,
			DELETE_ROWS_EVENTv2:
			if re, ok := ev.Event.(*RowsEvent); ok {
				err = psql.processDeleteRow(re)
				check(err)
			}
		case UPDATE_ROWS_EVENTv0,
			UPDATE_ROWS_EVENTv1,
			UPDATE_ROWS_EVENTv2:
			if re, ok := ev.Event.(*RowsEvent); ok {
				err = psql.processUpdateRow(re)
				check(err)
			}
		}

		fmt.Printf("Events: %8d, Entries: %8d\n", syncer.count, psql.recordCount)
	}

	elapsed := time.Since(start).Milliseconds()
	fmt.Printf("Number of Binlog Events processed: %d\n", syncer.count)
	fmt.Printf("Number of entries processed: %d\n", psql.recordCount)
	fmt.Printf("Time used: %d ms\n", elapsed)

	nextPos := syncer.getNextPosition()
	err = writePos(nextPos)
	check(err)

}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
