package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/lib/pq"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	mysql_serverID = 1
	flavor         = "mysql"
	mysql_host     = "127.0.0.1"
	mysql_port     = 3306
	mysql_user     = "root"
	mysql_password = "k4aTJCcB4j=+"

	pq_host     = "localhost"
	pq_port     = 5432
	pq_user     = "postgres"
	pq_password = "k4aTJCcB4j=+"
)

var pq_dbname = "postgres"

func main() {
	fileName := "replication.conf"

	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		initial := &mysql.Position{Name: "binlog.000001", Pos: 0}
		content, err := json.MarshalIndent(initial, "", "\t")
		check(err)
		err = os.WriteFile(fileName, content, 0644)
		check(err)
	}

	data, err := os.ReadFile(fileName)
	check(err)
	pos := mysql.Position{}
	err = json.Unmarshal(data, &pos)
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
	streamer, _ := syncer.StartSync(pos)

	// psql connection
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		pq_host,
		pq_port,
		pq_user,
		pq_password,
		pq_dbname,
	)

	psql, err := sql.Open("postgres", psqlconn)
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
				schema := string(qe.Schema)
				if schema == "" {
					continue
				}

				if pq_dbname != schema {
					psql.Close()
					// close old connection, create new connection to psql using 'schema'
					pq_dbname = schema
					psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
						pq_host,
						pq_port,
						pq_user,
						pq_password,
						pq_dbname,
					)
					psql, err = sql.Open("postgres", psqlconn)
					check(err)
					defer psql.Close()

					// check if new schema exists, if not, create it
					err = psql.Ping()
					if err != nil {
						if pqerr, ok := err.(*pq.Error); ok {
							if pqerr.Code == "3D000" {
								// database not exist in pqsl
								// create new database using default user postgres
								psql.Close()
								pq_dbname = "postgres"
								psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
									pq_host,
									pq_port,
									pq_user,
									pq_password,
									pq_dbname,
								)
								psql, err = sql.Open("postgres", psqlconn)
								check(err)
								defer psql.Close()
								_, err = psql.Exec("create database " + schema)
								check(err)
								psql.Close()

								// connect to the new created database
								pq_dbname = schema
								psqlconn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
									pq_host,
									pq_port,
									pq_user,
									pq_password,
									pq_dbname,
								)
								psql, err = sql.Open("postgres", psqlconn)
								check(err)
								defer psql.Close()

								pingErr := psql.Ping()
								check(pingErr)
							} else {
								panic(pqerr)
							}
						}
					}
				} else {
					// use current connection
					pingErr := psql.Ping()
					check(pingErr)
				}
			}
		case replication.TABLE_MAP_EVENT:
			if tme, ok := ev.Event.(*replication.TableMapEvent); ok {
				fmt.Println("--table map event--")
				fmt.Printf("table: %s\n", tme.Table)
				fmt.Printf("schema: %s\n", tme.Schema)
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
			}
		}
	}

	content, err := json.MarshalIndent(nextPos, "", "\t")
	check(err)
	err = os.WriteFile(fileName, content, 0644)
	check(err)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
