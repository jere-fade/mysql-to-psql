package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	mysql_serverID = 1
	flavor         = "mysql"
	mysql_host     = "127.0.0.1"
	mysql_port     = 3306

	pq_host      = "localhost"
	pq_port      = 5432
	pq_defaultDB = "postgres"
)

var mysql_user = os.Getenv("mysql_user")
var mysql_password = os.Getenv("mysql_password")
var pq_user = os.Getenv("pq_user")
var pq_password = os.Getenv("pq_password")

func main() {

	if mysql_user == "" {
		panic("mysql user not set")
	}
	if mysql_password == "" {
		panic("mysql password not set")
	}
	if pq_user == "" {
		panic("pqsl user not set")
	}
	if pq_password == "" {
		panic("pqsl password not set")
	}

	pos, err := readPos()
	check(err)

	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	cfg := replication.BinlogSyncerConfig{
		ServerID: mysql_serverID,
		Flavor:   flavor,
		Host:     mysql_host,
		Port:     mysql_port,
		User:     mysql_user,
		Password: mysql_password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	nextPos := syncer.GetNextPosition()

	// Start sync with specified binlog file and position
	streamer, _ := syncer.StartSync(*pos)

	// psql connection
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		pq_host,
		pq_port,
		pq_user,
		pq_password,
		pq_defaultDB,
	)

	psql := &PsqlConn{pq_dbname: pq_defaultDB}
	psql.conn, err = sql.Open("postgres", psqlconn)
	check(err)
	defer psql.Close()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ev, err := streamer.GetEvent(ctx)
		cancel()

		if err == context.DeadlineExceeded {
			break
		}

		ev.Dump(os.Stdout)
		switch ev.Header.EventType {
		case replication.ROTATE_EVENT:
			if re, ok := ev.Event.(*replication.RotateEvent); ok {
				nextPos.Name = string(re.NextLogName)
				nextPos.Pos = uint32(re.Position)
			}
		case replication.QUERY_EVENT:
			if qe, ok := ev.Event.(*replication.QueryEvent); ok {
				nextPos.Pos = ev.Header.LogPos
				fmt.Println("--query event--")
				fmt.Printf("schema: %s\n", qe.Schema)
				fmt.Printf("query: %s\n", qe.Query)
				err = psql.processQuery(qe)
				check(err)
			}
		case replication.TABLE_MAP_EVENT:
			if tme, ok := ev.Event.(*replication.TableMapEvent); ok {
				fmt.Println("--table map event--")
				fmt.Printf("table: %s\n", tme.Table)
				fmt.Printf("schema: %s\n", tme.Schema)
				fmt.Printf("column count: %d\n", tme.ColumnCount)
				columnName := tme.ColumnNameString()
				if columnName == nil {
					fmt.Println("fuck")
				}
				for i, name := range columnName {
					fmt.Printf("column name: %d %s\n", i, name)
				}
			}
		case replication.WRITE_ROWS_EVENTv0,
			replication.UPDATE_ROWS_EVENTv0,
			replication.DELETE_ROWS_EVENTv0,
			replication.WRITE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv1,
			replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv2:
			if re, ok := ev.Event.(*replication.RowsEvent); ok {
				fmt.Println("--rows event--")
				fmt.Printf("version: %d\n", re.Version)
				fmt.Printf("table: %s\n", re.Table.Table)
			}
		}
	}

	err = writePos(nextPos)
	check(err)

}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
