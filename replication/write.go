package replication

import (
	"fmt"
	"strings"
)

func (pc *PsqlConn) processWriteRow(re *RowsEvent) error {
	writeValues := re.Rows
	tableName := string(re.Table.Table)
	sqlSlice := make([]string, len(writeValues))

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
		pc.recordCount++
	}

	for i := 0; i < len(sqlSlice); i++ {
		_, err := pc.conn.Exec(sqlSlice[i])
		// fmt.Println(sqlSlice[i])
		if err != nil {
			return err
		}
	}
	return nil
}
