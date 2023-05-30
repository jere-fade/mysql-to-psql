package main

import (
	"fmt"
	"strings"
	"database/sql"
	"github.com/go-mysql-org/go-mysql/replication"
	// "github.com/lib/pq"
)

func (pc *PsqlConn) processWriteRow(re *replication.RowsEvent) error {
	writeValues := re.Rows
	tableName := string(re.Table.Table)
	sqlSlice := make([]string, len(writeValues))
	schema := string(re.Table.Schema)

	for i := 0; i < len(writeValues); i++ {
		stringValues := make([]string, len(writeValues[i]))
		for j, v := range writeValues[i] {
			switch v.(type) {
			default:
				// fmt.Printf("unexpected type %T", t)
				stringValues[j] = fmt.Sprintf("%v", v)
			case string:
				stringValues[j] = fmt.Sprintf("'%v'", v)
			}
		}
		sqlSlice[i] = "INSERT INTO " + tableName + " VALUES (" + strings.Join(stringValues, ", ") + ");"
	}
	if pc.pq_dbname != schema {
		pc.conn.Close()
		// close old connection, create new connection to psql using 'schema'
		pc.pq_dbname = schema
		psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			pq_host,
			pq_port,
			pq_user,
			pq_password,
			pc.pq_dbname,
		)
		var err error
		pc.conn, err = sql.Open("postgres", psqlconn)
		if err != nil {
			return err
		}
	} else {
		// use current connection
		pingErr := pc.conn.Ping()
		if pingErr != nil {
			return pingErr
		}
	}

	for i := 0; i < len(sqlSlice); i++ {
		// _, err := pc.conn.Exec(sqlSlice[i])
		_, err := pc.conn.Exec("SHOW search_path;select * from student;")
		fmt.Println(sqlSlice[i])
		if err != nil {
			return err
		}
	}
	return nil
}
