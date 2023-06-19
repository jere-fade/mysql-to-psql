package replication

import (
	"encoding/json"
	"errors"
	"os"
)

const fileName = "replication.conf"

func readPos() (*Position, error) {
	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		initial := Position{Name: "binlog.000001", Pos: 0}
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
	pos := &Position{}
	err = json.Unmarshal(data, pos)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func writePos(nextPos Position) error {
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
