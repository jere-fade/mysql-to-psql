package main

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/go-mysql-org/go-mysql/mysql"
)

const fileName = "replication.conf"

func readPos() (*mysql.Position, error) {
	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		initial := mysql.Position{Name: "", Pos: 0}
		content, err := json.MarshalIndent(initial, "", "\t")
		if err != nil {
			return nil, err
		}
		err = os.WriteFile(fileName, content, 0644)
		if err != nil {
			return nil, err
		}
		return nil, errors.New("replication.conf not found, please set correct binlog file position in it")
	}
	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	pos := &mysql.Position{}
	err = json.Unmarshal(data, pos)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func writePos(nextPos mysql.Position) error {
	content, err := json.MarshalIndent(nextPos, "", "\t")
	if err != nil {
		return err
	}
	err = os.WriteFile(fileName, content, 0644)
	if err != nil {
		return err
	}
	return nil
}
