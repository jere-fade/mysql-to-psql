package main

import (
	"fmt"
)

func (pc *PsqlConn) processUpdateRow(re *RowsEvent) error {
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

	baseDeleteSql := fmt.Sprintf("DELETE FROM %s WHERE", tableName)
	baseInsertSql := fmt.Sprintf("INSERT INTO %s VALUES ( ", tableName)
	for i, row := range re.Rows {
		if i%2 == 0 {
			deleteSql := baseDeleteSql
			for j, column := range row {
				var condition string
				switch column.(type) {
				case string:
					condition = fmt.Sprintf(" %v = '%v'", columnNames[j], column)
				default:
					condition = fmt.Sprintf(" %v = %v", columnNames[j], column)
				}
				deleteSql += condition

				if j != len(row)-1 {
					deleteSql += " AND"
				}
			}

			fmt.Println(deleteSql)
			_, err = pc.conn.Exec(deleteSql)
			if err != nil {
				return err
			}
		} else {
			insertSql := baseInsertSql
			for j, column := range row {
				var value string
				switch column.(type) {
				case string:
					value = fmt.Sprintf("'%v'", column)
				default:
					value = fmt.Sprintf("%v", column)
				}
				insertSql += value

				if j != len(row)-1 {
					insertSql += ", "
				} else {
					insertSql += " )"
				}
			}

			fmt.Println(insertSql)
			_, err = pc.conn.Exec(insertSql)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
