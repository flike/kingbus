package mysql

import (
	"strings"

	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/utils"
	"github.com/siddontang/go-mysql/replication"
)

//EventBoundaryType is the type of binlog event boundary
type EventBoundaryType int8

const (
	//EventBoundaryTypeError represents error
	EventBoundaryTypeError EventBoundaryType = iota

	//EventBoundaryTypeGtid represents Gtid_log_event
	EventBoundaryTypeGtid

	//EventBoundaryTypeBeginTrx represents Query_log_event(BEGIN) or Query_log_event(XA START)
	EventBoundaryTypeBeginTrx

	//EventBoundaryTypeEndTrx represents Xid, Query_log_event(COMMIT), Query_log_event(ROLLBACK), XA_Prepare_log_event
	EventBoundaryTypeEndTrx

	//EventBoundaryTypeEndXaTrx represents Query_log_event(XA ROLLBACK)
	EventBoundaryTypeEndXaTrx

	//EventBoundaryTypePreStatement represents User_var, Intvar and Rand
	EventBoundaryTypePreStatement

	//EventBoundaryTypeStatement represents All other Query_log_events and all other DML events
	// (Rows, Load_data, etc.)
	EventBoundaryTypeStatement

	//EventBoundaryTypeIncident represents Incident
	EventBoundaryTypeIncident

	//EventBoundaryTypeIgnore represents All non DDL/DML events: Format_desc, Rotate,
	//Previous_gtids, Stop, etc.
	EventBoundaryTypeIgnore
)

//Internal states for parsing a stream of events.
//
//    DDL has the format:
//      DDL-1: [GTID]
//      DDL-2: [User] [Intvar] [Rand]
//      DDL-3: Query
//
//    DML has the format:
//      DML-1: [GTID]
//      DML-2: Query(BEGIN)
//      DML-3: Statements
//      DML-4: (Query(COMMIT) | Query([XA] ROLLBACK) | Xid | Xa_prepare)

type eventParserState int8

const (
	// EventParserNone is set after DDL-3 or DML-4
	EventParserNone eventParserState = iota
	// EventParserGtid is set after DDL-1 or DML-1
	EventParserGtid
	// EventParserDDL is set after DDL-2
	EventParserDDL
	// EventParserDML is set after DML-2
	EventParserDML
	// EventParserError is set whenever the above pattern is not followed
	EventParserError
)

var parserStateString = map[eventParserState]string{
	EventParserNone:  "None",
	EventParserGtid:  "GTID",
	EventParserDDL:   "DDL",
	EventParserDML:   "DML",
	EventParserError: "ERROR",
}

//TransactionBoundaryParser represents a transaction boundary parser
type TransactionBoundaryParser struct {
	currentState eventParserState
}

//IsInsideTransaction return true if current state is in transaction
func (p *TransactionBoundaryParser) IsInsideTransaction() bool {
	return p.currentState != EventParserError &&
		p.currentState != EventParserNone
}

//IsNotInsideTransaction return true if current state is not in transaction
func (p *TransactionBoundaryParser) IsNotInsideTransaction() bool {
	return p.currentState == EventParserNone
}

//IsError return true if current state is error
func (p *TransactionBoundaryParser) IsError() bool {
	return p.currentState == EventParserError
}

//Reset TransactionBoundaryParser
func (p *TransactionBoundaryParser) Reset() {
	p.currentState = EventParserNone
}

//GetEventBoundaryType implements
func (p *TransactionBoundaryParser) GetEventBoundaryType(h *replication.EventHeader,
	eventBody []byte) (EventBoundaryType, error) {

	var err error
	var boundaryType = EventBoundaryTypeError
	switch h.EventType {
	case replication.GTID_EVENT, replication.ANONYMOUS_GTID_EVENT:
		boundaryType = EventBoundaryTypeGtid

	case replication.QUERY_EVENT:
		//There are four types of queries that we have to deal with: BEGIN, COMMIT,
		//ROLLBACK and the rest.
		e := &replication.QueryEvent{}
		err = e.Decode(eventBody)
		if err != nil {
			return EventBoundaryTypeError, err
		}
		query := strings.ToUpper(utils.BytesToString(e.Query))

		log.Log.Debugf("query is:%s", query)

		if query == "BEGIN" || query == "XA START" {
			//BEGIN is always the begin of a DML transaction
			boundaryType = EventBoundaryTypeBeginTrx
		} else if strings.HasPrefix(query, "ROLLBACK") || strings.HasPrefix(query, "ROLLBACK TO ") {
			//COMMIT and ROLLBACK are always the end of a transaction
			boundaryType = EventBoundaryTypeEndTrx
		} else if strings.HasPrefix(query, "XA ROLLBACK") {
			//XA ROLLBACK is always the end of a XA transaction
			boundaryType = EventBoundaryTypeEndXaTrx
		} else {
			//If the query is not (BEGIN | XA START | COMMIT | [XA] ROLLBACK), it can
			//be considered an ordinary statement
			boundaryType = EventBoundaryTypeStatement
		}

	case replication.XID_EVENT:
		boundaryType = EventBoundaryTypeEndTrx

	case replication.XA_PREPARE_LOG_EVENT:
		boundaryType = EventBoundaryTypeEndTrx

	case replication.INTVAR_EVENT, replication.RAND_EVENT, replication.USER_VAR_EVENT:
		boundaryType = EventBoundaryTypePreStatement

	case replication.EXECUTE_LOAD_QUERY_EVENT, replication.TABLE_MAP_EVENT,
		replication.APPEND_BLOCK_EVENT, replication.BEGIN_LOAD_QUERY_EVENT,
		replication.ROWS_QUERY_EVENT, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2,
		replication.WRITE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv0,
		replication.VIEW_CHANGE_EVENT:

		boundaryType = EventBoundaryTypeStatement

	case replication.INCIDENT_EVENT:
		boundaryType = EventBoundaryTypeIncident

	case replication.ROTATE_EVENT, replication.FORMAT_DESCRIPTION_EVENT,
		replication.HEARTBEAT_EVENT, replication.PREVIOUS_GTIDS_EVENT,
		replication.START_EVENT_V3, replication.STOP_EVENT,
		replication.LOAD_EVENT, replication.SLAVE_EVENT,
		replication.CREATE_FILE_EVENT, replication.DELETE_FILE_EVENT,
		replication.NEW_LOAD_EVENT, replication.EXEC_LOAD_EVENT,
		replication.TRANSACTION_CONTEXT_EVENT:

		boundaryType = EventBoundaryTypeIgnore

	default:
		if h.Flags == replication.LOG_EVENT_IGNORABLE_F {
			boundaryType = EventBoundaryTypeIgnore
		} else {
			log.Log.Warnf("Unsupported non-ignorable event fed into the event stream")
			boundaryType = EventBoundaryTypeError
		}
	}
	return boundaryType, nil
}

//UpdateState TransactionBoundaryParser state
func (p *TransactionBoundaryParser) UpdateState(eventBoundaryType EventBoundaryType) error {
	var newParserState = EventParserNone
	var err error

	switch eventBoundaryType {
	case EventBoundaryTypeGtid:
		newParserState = EventParserGtid
		switch p.currentState {
		case EventParserGtid, EventParserDDL, EventParserDML:
			if p.currentState == EventParserGtid {
				log.Log.Warnf("GTID_LOG_EVENT or ANONYMOUS_GTID_LOG_EVENT is not" +
					" expected in an event stream after a GTID_LOG_EVENT or an ANONYMOUS_GTID_LOG_EVENT")
			} else if p.currentState == EventParserDDL {
				log.Log.Warnf("GTID_LOG_EVENT or ANONYMOUS_GTID_LOG_EVENT is not " +
					"expected in an event stream after in the middle of a DDL")
			} else {
				log.Log.Warnf("GTID_LOG_EVENT or ANONYMOUS_GTID_LOG_EVENT is not " +
					"expected in an event stream after in the middle of a DML")
			}
			err = ErrUpdateState
		case EventParserError:
			err = ErrUpdateState
		case EventParserNone:
		}

	case EventBoundaryTypeBeginTrx:
		newParserState = EventParserDML
		switch p.currentState {
		case EventParserDDL, EventParserDML:
			if p.currentState == EventParserDDL {
				log.Log.Warnf("QUERY(BEGIN) is not expected " +
					"in an event stream in the middle of a DDL")
			} else {
				log.Log.Warnf("QUERY(BEGIN) is not expected " +
					"in an event stream in the middle of a DML")
			}
			err = ErrUpdateState
		case EventParserError:
			err = ErrUpdateState
		case EventParserNone, EventParserGtid:
		}

	case EventBoundaryTypeEndTrx:
		newParserState = EventParserNone
		switch p.currentState {
		case EventParserNone, EventParserGtid, EventParserDDL:
			if p.currentState == EventParserNone {
				log.Log.Warnf("QUERY(COMMIT or ROLLBACK) or " +
					"XID_LOG_EVENT is not expected in an event stream outside a transaction")
			} else if p.currentState == EventParserGtid {
				log.Log.Warnf("QUERY(COMMIT or ROLLBACK) or " +
					"XID_LOG_EVENT is not expected in an event stream after a GTID_LOG_EVENT")
			} else {
				log.Log.Warnf("QUERY(COMMIT or ROLLBACK) or " +
					"XID_LOG_EVENT is not expected in an event stream in the middle of a DDL")
			}
			err = ErrUpdateState
		case EventParserError:
			err = ErrUpdateState
		case EventParserDML:
		}

	case EventBoundaryTypeEndXaTrx:
		newParserState = EventParserNone
		switch p.currentState {
		case EventParserNone, EventParserDDL:
			if p.currentState == EventParserNone {
				log.Log.Warnf("QUERY(XA ROLLBACK) is not expected in an event stream outside a transaction")
			} else {
				log.Log.Warnf("QUERY(XA ROLLBACK) is not expected in an event stream in the middle of a DDL")
			}
			err = ErrUpdateState
		case EventParserError:
			err = ErrUpdateState
		case EventParserDML, EventParserGtid:
		}

	case EventBoundaryTypeStatement:
		switch p.currentState {
		case EventParserNone:
			newParserState = EventParserNone
		case EventParserGtid, EventParserDDL:
			newParserState = EventParserNone
		case EventParserDML:
			newParserState = p.currentState
		case EventParserError:
			err = ErrUpdateState
		}

	case EventBoundaryTypePreStatement:
		switch p.currentState {
		case EventParserNone, EventParserGtid:
			newParserState = EventParserDDL
		case EventParserDDL, EventParserDML:
			newParserState = p.currentState
		case EventParserError:
			err = ErrUpdateState
		}

	case EventBoundaryTypeIncident:
		newParserState = EventParserNone

	case EventBoundaryTypeIgnore:
		newParserState = p.currentState

	case EventBoundaryTypeError:
		newParserState = EventParserError
		err = ErrUpdateState
	}

	//log.Log.Debugf("transaction boundary parser is changing state from %s to %s, eventBoundaryType:%v",
	//	parserStateString[p.currentState], parserStateString[newParserState], eventBoundaryType)

	p.currentState = newParserState
	return err
}
