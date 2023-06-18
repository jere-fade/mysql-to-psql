package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
)

const logFolder string = "log"
const headerLength uint32 = 19

type Syncer struct {
	Position Position
	Host     string
	Port     uint16
	User     string
	Password string
}

type Position struct {
	Name string
	Pos  uint32
}

func (s *Syncer) syncLog() error {
	err := os.MkdirAll(logFolder, 0744)
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
			index, err := strconv.Atoi(strings.ReplaceAll(s.Position.Name, "binlog.", ""))
			if err != nil {
				return nil, err
			} else {
				newLogName := fmt.Sprintf("binlog.%06d", index+1)
				if _, err = os.Stat(path.Join(logFolder, newLogName)); errors.Is(err, os.ErrNotExist) {
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

	var e Event
	switch header.EventType {
	case QUERY_EVENT:
		e := &QueryEvent{}
		e.parse(bodyBuffer)
		return &BinlogEvent{EventHeader: header, Event: e}, nil
	}

	return &BinlogEvent{EventHeader: header, Event: e}, nil

}
