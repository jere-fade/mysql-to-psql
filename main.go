package main

import (
	"database/sql"
	"fmt"
	"os"
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

	pos, err := readPos()
	check(err)

	syncer := &Syncer{
		Position: *pos,
		Host:     mysql_host,
		Port:     mysql_port,
		User:     mysql_user,
		Password: mysql_password,
	}

	err = syncer.syncLog()
	check(err)

	// cfg := replication.BinlogSyncerConfig{
	// 	ServerID: mysql_serverID,
	// 	Flavor:   flavor,
	// 	Host:     mysql_host,
	// 	Port:     mysql_port,
	// 	User:     mysql_user,
	// 	Password: mysql_password,
	// }
	// syncer := replication.NewBinlogSyncer(cfg)
	// nextPos := syncer.GetNextPosition()

	// Start sync with specified binlog file and position
	// streamer, _ := syncer.StartSync(*pos)
	// streamer, _ := syncer.StartSync(mysql.Position{Name: pos.Name, Pos: pos.Pos})

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
	defer psql.conn.Close()

	for {
		ev, err := syncer.getEvent()

		if err != nil {
			check(err)
		} else if ev == nil {
			break
		}

		// ev.Dump(os.Stdout)
		switch ev.EventHeader.EventType {
		// case replication.ROTATE_EVENT:
		// 	if re, ok := ev.Event.(*replication.RotateEvent); ok {
		// 		nextPos.Name = string(re.NextLogName)
		// 		nextPos.Pos = uint32(re.Position)
		// 	}
		case QUERY_EVENT:
			if qe, ok := ev.Event.(*QueryEvent); ok {
				err = psql.processQuery(qe)
				check(err)
			}
			// case replication.TABLE_MAP_EVENT:
			// 	if tme, ok := ev.Event.(*replication.TableMapEvent); ok {

			// 	}
			// case replication.WRITE_ROWS_EVENTv0,
			// 	replication.WRITE_ROWS_EVENTv1,
			// 	replication.WRITE_ROWS_EVENTv2:
			// 	if re, ok := ev.Event.(*replication.RowsEvent); ok {
			// 		err = psql.processWriteRow(re)
			// 		check(err)
			// 	}
			// case replication.DELETE_ROWS_EVENTv0,
			// 	replication.DELETE_ROWS_EVENTv1,
			// 	replication.DELETE_ROWS_EVENTv2:
			// 	if re, ok := ev.Event.(*replication.RowsEvent); ok {
			// 		err = psql.processDeleteRow(re)
			// 		check(err)
			// 	}
			// case replication.UPDATE_ROWS_EVENTv0,
			// 	replication.UPDATE_ROWS_EVENTv1,
			// 	replication.UPDATE_ROWS_EVENTv2:
			// 	if re, ok := ev.Event.(*replication.RowsEvent); ok {
			// 		err = psql.processUpdateRow(re)
			// 		check(err)
			// 	}
		}
	}

	// nextPos := syncer.getNextPosition()
	// err = writePos(nextPos)
	// err = writePos(nextPos)
	// check(err)

}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
