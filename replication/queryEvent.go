package replication

import "encoding/binary"

type QueryEvent struct {
	SlaveProxyID  uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []byte
	Schema        []byte
	Query         []byte
}

func (qe *QueryEvent) parse(data []byte) error {

	pos := 0

	qe.SlaveProxyID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	qe.ExecutionTime = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	schemaLength := data[pos]
	pos++

	qe.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	statusVarsLength := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	qe.StatusVars = data[pos : pos+int(statusVarsLength)]
	pos += int(statusVarsLength)

	qe.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	pos++

	qe.Query = data[pos:]

	return nil
}
