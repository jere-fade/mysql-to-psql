package main

type BinlogEvent struct {
	EventHeader EventHeader
	Event       Event
}

type EventHeader struct {
	Timestamp uint32
	EventType EventType
	ServerID  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

type Event interface {
	parse(data []byte) error
}

type EventType byte

const (
	UNKNOWN_EVENT EventType = iota
	START_EVENT_V3
	QUERY_EVENT
	STOP_EVENT
	ROTATE_EVENT
	INTVAR_EVENT
	LOAD_EVENT
	SLAVE_EVENT
	CREATE_FILE_EVENT
	APPEND_BLOCK_EVENT
	EXEC_LOAD_EVENT
	DELETE_FILE_EVENT
	NEW_LOAD_EVENT
	RAND_EVENT
	USER_VAR_EVENT
	FORMAT_DESCRIPTION_EVENT
	XID_EVENT
	BEGIN_LOAD_QUERY_EVENT
	EXECUTE_LOAD_QUERY_EVENT
	TABLE_MAP_EVENT
	WRITE_ROWS_EVENTv0
	UPDATE_ROWS_EVENTv0
	DELETE_ROWS_EVENTv0
	WRITE_ROWS_EVENTv1
	UPDATE_ROWS_EVENTv1
	DELETE_ROWS_EVENTv1
	INCIDENT_EVENT
	HEARTBEAT_EVENT
	IGNORABLE_EVENT
	ROWS_QUERY_EVENT
	WRITE_ROWS_EVENTv2
	UPDATE_ROWS_EVENTv2
	DELETE_ROWS_EVENTv2
	GTID_EVENT
	ANONYMOUS_GTID_EVENT
	PREVIOUS_GTIDS_EVENT
	TRANSACTION_CONTEXT_EVENT
	VIEW_CHANGE_EVENT
	XA_PREPARE_LOG_EVENT
	PARTIAL_UPDATE_ROWS_EVENT
	TRANSACTION_PAYLOAD_EVENT
	HEARTBEAT_LOG_EVENT_V2
)

func (e EventType) String() string {
	switch e {
	case UNKNOWN_EVENT:
		return "UnknownEvent"
	case START_EVENT_V3:
		return "StartEventV3"
	case QUERY_EVENT:
		return "QueryEvent"
	case STOP_EVENT:
		return "StopEvent"
	case ROTATE_EVENT:
		return "RotateEvent"
	case INTVAR_EVENT:
		return "IntVarEvent"
	case LOAD_EVENT:
		return "LoadEvent"
	case SLAVE_EVENT:
		return "SlaveEvent"
	case CREATE_FILE_EVENT:
		return "CreateFileEvent"
	case APPEND_BLOCK_EVENT:
		return "AppendBlockEvent"
	case EXEC_LOAD_EVENT:
		return "ExecLoadEvent"
	case DELETE_FILE_EVENT:
		return "DeleteFileEvent"
	case NEW_LOAD_EVENT:
		return "NewLoadEvent"
	case RAND_EVENT:
		return "RandEvent"
	case USER_VAR_EVENT:
		return "UserVarEvent"
	case FORMAT_DESCRIPTION_EVENT:
		return "FormatDescriptionEvent"
	case XID_EVENT:
		return "XIDEvent"
	case BEGIN_LOAD_QUERY_EVENT:
		return "BeginLoadQueryEvent"
	case EXECUTE_LOAD_QUERY_EVENT:
		return "ExectueLoadQueryEvent"
	case TABLE_MAP_EVENT:
		return "TableMapEvent"
	case WRITE_ROWS_EVENTv0:
		return "WriteRowsEventV0"
	case UPDATE_ROWS_EVENTv0:
		return "UpdateRowsEventV0"
	case DELETE_ROWS_EVENTv0:
		return "DeleteRowsEventV0"
	case WRITE_ROWS_EVENTv1:
		return "WriteRowsEventV1"
	case UPDATE_ROWS_EVENTv1:
		return "UpdateRowsEventV1"
	case DELETE_ROWS_EVENTv1:
		return "DeleteRowsEventV1"
	case INCIDENT_EVENT:
		return "IncidentEvent"
	case HEARTBEAT_EVENT:
		return "HeartbeatEvent"
	case IGNORABLE_EVENT:
		return "IgnorableEvent"
	case ROWS_QUERY_EVENT:
		return "RowsQueryEvent"
	case WRITE_ROWS_EVENTv2:
		return "WriteRowsEventV2"
	case UPDATE_ROWS_EVENTv2:
		return "UpdateRowsEventV2"
	case DELETE_ROWS_EVENTv2:
		return "DeleteRowsEventV2"
	case GTID_EVENT:
		return "GTIDEvent"
	case ANONYMOUS_GTID_EVENT:
		return "AnonymousGTIDEvent"
	case PREVIOUS_GTIDS_EVENT:
		return "PreviousGTIDsEvent"
	case TRANSACTION_CONTEXT_EVENT:
		return "TransactionContextEvent"
	case VIEW_CHANGE_EVENT:
		return "ViewChangeEvent"
	case XA_PREPARE_LOG_EVENT:
		return "XAPrepareLogEvent"
	case PARTIAL_UPDATE_ROWS_EVENT:
		return "PartialUpdateRowsEvent"
	case TRANSACTION_PAYLOAD_EVENT:
		return "TransactionPayloadEvent"
	case HEARTBEAT_LOG_EVENT_V2:
		return "HeartbeatLogEventV2"
	default:
		return "UnknownEvent"
	}
}
