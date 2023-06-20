package replication

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
)

const logFolder string = "log"
const headerLength uint32 = 19

type Syncer struct {
	Position Position
	Host     string
	Port     uint16
	User     string
	Password string
	tables   map[uint64]*TableMapEvent
	count    int
}

type Position struct {
	Name string
	Pos  uint32
}

const repConfigFileName = "replication.conf"

func (s *Syncer) getDefaultLogPos() (*Position, error) {
	cfg := mysql.Config{
		User:   s.User,
		Passwd: s.Password,
		Addr:   s.Host + ":" + fmt.Sprintf("%d", s.Port),
	}

	var err error
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	pingErr := db.Ping()
	if pingErr != nil {
		return nil, pingErr
	}

	var logName string
	var pos uint32
	err = db.QueryRow("select JSON_EXTRACT(`LOCAL`, '$.binary_log_file'), JSON_EXTRACT(`LOCAL`, '$.binary_log_position') from performance_schema.log_status").Scan(&logName, &pos)
	if err != nil {
		return nil, err
	}
	logName = strings.ReplaceAll(logName, "\"", "")
	return &Position{Name: logName, Pos: pos}, nil

}

func (s *Syncer) syncLog() error {
	if _, err := os.Stat(repConfigFileName); errors.Is(err, os.ErrNotExist) {
		defaultPos, err := s.getDefaultLogPos()
		if err != nil {
			return err
		}
		initial := *defaultPos
		content, err := json.MarshalIndent(initial, "", "\t")
		if err != nil {
			return err
		}
		err = os.WriteFile(repConfigFileName, content, 0644)
		if err != nil {
			return err
		}
		return errors.New("please set correct binlog file position in replication.conf")
	}

	data, err := os.ReadFile(repConfigFileName)
	if err != nil {
		return err
	}
	pos := &Position{}
	err = json.Unmarshal(data, pos)
	if err != nil {
		return err
	}
	s.Position = *pos

	err = os.MkdirAll(logFolder, 0744)
	if err != nil {
		return err
	}
	err = os.Chdir(logFolder)
	if err != nil {
		return err
	}
	cmd := exec.Command("mysqlbinlog", "-R", "-h", s.Host, "-u", s.User, "--password="+s.Password, "--raw", "-t", s.Position.Name)
	err = cmd.Run()
	if err != nil {
		return err
	}
	err = os.Chdir("..")
	if err != nil {
		return err
	}
	return nil
}

func (s *Syncer) getNextPosition() Position {
	return s.Position
}

func (s *Syncer) getEvent() (*BinlogEvent, error) {
	logFile, err := os.Open(path.Join(logFolder, s.Position.Name))
	if err != nil {
		return nil, err
	}
	defer logFile.Close()

	if s.Position.Pos < 4 {
		logFile.Seek(4, 1)
	} else {
		logFile.Seek(int64(s.Position.Pos), 1)
	}
	headerBuffer := make([]byte, headerLength)
	_, err = logFile.Read(headerBuffer)
	if err != nil {
		// current log file read to end, judge if next log file exists
		if err == io.EOF {
			index, err := strconv.Atoi(strings.ReplaceAll(filepath.Ext(s.Position.Name), ".", ""))
			if err != nil {
				return nil, err
			} else {
				newLogName := fmt.Sprintf("%s.%06d", strings.TrimSuffix(s.Position.Name, filepath.Ext(s.Position.Name)), index+1)
				if _, err = os.Stat(path.Join(logFolder, newLogName)); errors.Is(err, os.ErrNotExist) {
					content, err := json.MarshalIndent(s.Position, "", "\t")
					if err != nil {
						return nil, err
					}
					err = os.WriteFile(repConfigFileName, content, 0644)
					if err != nil {
						return nil, err
					}
					return nil, nil
				} else {
					s.Position.Name = newLogName
					s.Position.Pos = 0
					logFile, err = os.Open(path.Join(logFolder, s.Position.Name))
					if err != nil {
						return nil, err
					}
					defer logFile.Close()
					if s.Position.Pos < 4 {
						logFile.Seek(4, 1)
					} else {
						logFile.Seek(int64(s.Position.Pos), 1)
					}
					_, err = logFile.Read(headerBuffer)
					if err != nil {
						return nil, err
					}
				}
			}
		} else {
			return nil, err
		}
	}

	header := EventHeader{}
	pos := 0

	header.Timestamp = binary.LittleEndian.Uint32(headerBuffer[pos:])
	pos += 4

	header.EventType = EventType(headerBuffer[pos])
	pos += 1

	header.ServerID = binary.LittleEndian.Uint32(headerBuffer[pos:])
	pos += 4

	header.EventSize = binary.LittleEndian.Uint32(headerBuffer[pos:])
	pos += 4

	header.LogPos = binary.LittleEndian.Uint32(headerBuffer[pos:])
	pos += 4

	s.Position.Pos = header.LogPos

	header.Flags = binary.LittleEndian.Uint16(headerBuffer[pos:])

	bodyBuffer := make([]byte, header.EventSize-headerLength)
	_, err = logFile.Read(bodyBuffer)
	if err != nil {
		return nil, err
	}

	if header.EventType != FORMAT_DESCRIPTION_EVENT {
		bodyBuffer = bodyBuffer[0 : len(bodyBuffer)-4]
	}

	var e Event
	switch header.EventType {
	case QUERY_EVENT:
		e := &QueryEvent{}
		e.parse(bodyBuffer)
		s.count++
		return &BinlogEvent{EventHeader: header, Event: e}, nil
	case TABLE_MAP_EVENT:
		e := &TableMapEvent{flavor: "mysql", tableIDSize: 6}
		e.parse(bodyBuffer)
		s.tables[e.TableID] = e
		s.count++
		return &BinlogEvent{EventHeader: header, Event: e}, nil
	case WRITE_ROWS_EVENTv0,
		WRITE_ROWS_EVENTv1,
		WRITE_ROWS_EVENTv2,
		DELETE_ROWS_EVENTv0,
		DELETE_ROWS_EVENTv1,
		DELETE_ROWS_EVENTv2,
		UPDATE_ROWS_EVENTv0,
		UPDATE_ROWS_EVENTv1,
		UPDATE_ROWS_EVENTv2:
		e := s.newRowsEvent(&header)
		e.parse(bodyBuffer)
		s.count++
		return &BinlogEvent{EventHeader: header, Event: e}, nil
	}

	return &BinlogEvent{EventHeader: header, Event: e}, nil

}

func (s *Syncer) newRowsEvent(h *EventHeader) *RowsEvent {
	e := &RowsEvent{}

	e.tableIDSize = 6
	e.needBitmap2 = false
	e.tables = s.tables
	e.eventType = h.EventType

	switch h.EventType {
	case WRITE_ROWS_EVENTv0:
		e.Version = 0
	case UPDATE_ROWS_EVENTv0:
		e.Version = 0
	case DELETE_ROWS_EVENTv0:
		e.Version = 0
	case WRITE_ROWS_EVENTv1:
		e.Version = 1
	case DELETE_ROWS_EVENTv1:
		e.Version = 1
	case UPDATE_ROWS_EVENTv1:
		e.Version = 1
		e.needBitmap2 = true
	case WRITE_ROWS_EVENTv2:
		e.Version = 2
	case UPDATE_ROWS_EVENTv2:
		e.Version = 2
		e.needBitmap2 = true
	case DELETE_ROWS_EVENTv2:
		e.Version = 2
	case PARTIAL_UPDATE_ROWS_EVENT:
		e.Version = 2
		e.needBitmap2 = true
	}
	return e
}
