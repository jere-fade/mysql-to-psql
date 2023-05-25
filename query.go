package main

import (
	"database/sql"
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/lib/pq"
)

type PsqlConn struct {
	conn      *sql.DB
	pq_dbname string
}

func (pc *PsqlConn) processQuery(qe *replication.QueryEvent) error {
	schema := string(qe.Schema)
	if schema == "" {
		return nil
	}

	// check if current connection is connected to the given schema
	if pc.pq_dbname != schema {
		pc.Close()
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

		// check if new schema exists, if not, create it
		err = pc.Ping()
		if err != nil {
			if pqerr, ok := err.(*pq.Error); ok {
				if pqerr.Code == "3D000" {
					// database not exist in pqsl
					// create new database using default user postgres
					pc.Close()
					adminPsqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
						pq_host,
						pq_port,
						pq_user,
						pq_password,
						pq_defaultDB,
					)
					adminPsql, err := sql.Open("postgres", adminPsqlconn)
					if err != nil {
						return err
					}
					_, err = adminPsql.Exec("create database " + schema)
					if err != nil {
						return err
					}
					adminPsql.Close()

					// check connecttion to the new created database
					pc.conn, err = sql.Open("postgres", psqlconn)
					if err != nil {
						return err
					}

					pingErr := pc.Ping()
					if pingErr != nil {
						return pingErr
					}
				} else {
					return err
				}
			}
		}
	} else {
		// use current connection
		pingErr := pc.Ping()
		if pingErr != nil {
			return pingErr
		}
	}

	// process the query

	return nil
}

func (pc *PsqlConn) Close() error {
	return pc.conn.Close()
}

func (pc *PsqlConn) Ping() error {
	return pc.conn.Ping()
}
