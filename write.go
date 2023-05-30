package main

import (
	"strings"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
)

func (pc *PsqlConn) processWriteRow(re *replication.RowsEvent) error {
	writeValues := re.Rows
	tableName := string(re.Table.Table)
	sqlSlice := make([]string, len(writeValues))

	for i := 0; i < len(writeValues); i++ {
		stringValues := make([]string, len(writeValues[i]))
		for j, v := range writeValues[i] {
			stringValues[j] = fmt.Sprintf("%v", v)
		}
		sqlSlice[i] = "INSERT INTO " + tableName + " VALUES (" + strings.Join(stringValues, ", ") + ")"
		fmt.Println(sqlSlice[i]);
	}
	return nil
}
