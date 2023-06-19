package replication

import (
	"fmt"
)

func (pc *PsqlConn) processDeleteRow(re *RowsEvent) error {
	tableName := string(re.Table.Table)
	columnNames := make([]string, 0)

	sql := `SELECT column_name FROM information_schema.columns
		WHERE table_name=$1 order by ordinal_position`
	result, err := pc.conn.Query(sql, tableName)
	if err != nil {
		return err
	}

	for result.Next() {
		var columnName string
		if err = result.Scan(&columnName); err != nil {
			return err
		}
		columnNames = append(columnNames, columnName)
	}

	baseSql := fmt.Sprintf("DELETE FROM %s WHERE", tableName)
	for _, row := range re.Rows {
		deleteSql := baseSql
		for i, column := range row {
			var condition string
			switch column.(type) {
			case string:
				condition = fmt.Sprintf(" %v = '%v'", columnNames[i], column)
			default:
				condition = fmt.Sprintf(" %v = %v", columnNames[i], column)
			}
			deleteSql += condition

			if i != len(row)-1 {
				deleteSql += " AND"
			}
		}
		pc.recordCount++
		// fmt.Println(deleteSql)
		_, err = pc.conn.Exec(deleteSql)
		if err != nil {
			return err
		}
	}

	return nil
}
