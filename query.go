package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/lib/pq"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	_ "github.com/pingcap/tidb/parser/test_driver"
)

type PsqlConn struct {
	conn      *sql.DB
	pq_dbname string
}

var adminPsqlconn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
	pq_host,
	pq_port,
	pq_user,
	pq_password,
	pq_defaultDB,
)

func (pc *PsqlConn) processQuery(qe *replication.QueryEvent) error {
	schema := string(qe.Schema)
	query := string(qe.Query)
	query = strings.ReplaceAll(query, "`", "")

	if schema == "" {
		return nil
	}

	// check if current connection is connected to the given schema
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

		// check if new schema exists, if not, create it
		err = pc.conn.Ping()
		if err != nil {
			if pqerr, ok := err.(*pq.Error); ok {
				if pqerr.Code == "3D000" {
					// database not exist in pqsl
					// create new database using default user postgres
					pc.conn.Close()

					adminPsql, err := sql.Open("postgres", adminPsqlconn)
					if err != nil {
						return err
					}

					if strings.Contains(strings.ToLower(query), "create database") {
						_, err = adminPsql.Exec(query)
					} else {
						_, err = adminPsql.Exec("create database " + schema)
					}
					if err != nil {
						return err
					}
					adminPsql.Close()

					// check connecttion to the new created database
					pc.conn, err = sql.Open("postgres", psqlconn)
					if err != nil {
						return err
					}

					pingErr := pc.conn.Ping()
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
		pingErr := pc.conn.Ping()
		if pingErr != nil {
			return pingErr
		}
	}

	// process the query
	// if query contain create database, skip
	queryLower := strings.ToLower(query)

	if strings.Contains(queryLower, "create database") {

		return nil

	} else if strings.Contains(queryLower, "drop database") {

		pc.conn.Close()
		adminPsql, err := sql.Open("postgres", adminPsqlconn)
		if err != nil {
			return err
		}
		_, err = adminPsql.Exec(query)
		if err != nil {
			return err
		}
		adminPsql.Close()

		pc.pq_dbname = pq_defaultDB
		psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			pq_host,
			pq_port,
			pq_user,
			pq_password,
			pc.pq_dbname,
		)
		pc.conn, err = sql.Open("postgres", psqlconn)
		if err != nil {
			return err
		}
		err = pc.conn.Ping()
		if err != nil {
			return err
		}

	} else if strings.Contains(queryLower, "create table") {

		pqQuery, err := parseCreateTable(query)

		if err != nil {
			return err
		}

		_, err = pc.conn.Exec(pqQuery)
		if err != nil {
			return err
		}

	} else if strings.Contains(queryLower, "drop table") {

		_, err := pc.conn.Exec(query)
		if err != nil {
			return err
		}

	} else {
		return nil
	}

	return nil
}

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

func parseCreateTable(sqlQuery string) (string, error) {
	astNode, err := parse(sqlQuery)
	if err != nil {
		return "", err
	}

	if ct, ok := (*astNode).(*ast.CreateTableStmt); ok {
		for _, col := range ct.Cols {
			i := 0
			for _, op := range col.Options {
				if op.Tp != ast.ColumnOptionAutoIncrement {
					col.Options[i] = op
					i += 1
				}
			}
			col.Options = col.Options[:i]
		}
		buf := new(bytes.Buffer)
		ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, buf)
		ct.Restore(ctx)
		return buf.String(), nil
	}

	return sqlQuery, nil
}
