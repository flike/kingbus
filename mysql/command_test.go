package mysql

import (
	"testing"

	"github.com/pingcap/tidb/types/parser_driver"

	"bytes"
	"os"
	"strings"

	"github.com/flike/kingbus/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/siddontang/go-mysql/replication"
	"github.com/stretchr/testify/assert"
)

const (
	TempLogDir = "/tmp/kingbus/data/log"
)

func init() {
	log.InitLoggers(TempLogDir, "DEBUG")
}

func TestParseGtidMode(t *testing.T) {
	sql := "SELECT @@global.gtid_mode"
	sqlParser := parser.New()
	stmt, err := sqlParser.ParseOneStmt(sql, "", "")
	assert.Nil(t, err)
	assert.IsType(t, stmt, &ast.SelectStmt{})

	selectStmt := stmt.(*ast.SelectStmt)
	assert.Equal(t, selectStmt.Fields.Fields[0].Text(), "@@global.gtid_mode")

	assert.IsType(t, selectStmt.Fields.Fields[0].Expr, &ast.VariableExpr{})

	expr := selectStmt.Fields.Fields[0].Expr.(*ast.VariableExpr)
	assert.Equal(t, expr.IsGlobal, true)
	assert.Equal(t, expr.IsSystem, true)

	assert.Equal(t, expr.Name, "gtid_mode")
}

func TestSetQuery(t *testing.T) {
	sql := "SET @master_binlog_checksum= @@global.binlog_checksum"
	sqlParser := parser.New()
	stmt, err := sqlParser.ParseOneStmt(sql, "", "")
	assert.Nil(t, err)
	assert.IsType(t, stmt, &ast.SetStmt{})

	setStmt := stmt.(*ast.SetStmt)
	assert.IsType(t, setStmt.Variables[0].Value, &ast.VariableExpr{})
	variableExpr := setStmt.Variables[0].Value.(*ast.VariableExpr)
	assert.Equal(t, "binlog_checksum", variableExpr.Name)
	assert.Equal(t, true, variableExpr.IsGlobal)
	assert.Equal(t, true, variableExpr.IsSystem)

	sql = "SET @master_binlog_checksum= 'CRC32'"
	sqlParser = parser.New()
	stmt, err = sqlParser.ParseOneStmt(sql, "", "")
	assert.Nil(t, err)
	assert.IsType(t, &ast.SetStmt{}, stmt)

	setStmt = stmt.(*ast.SetStmt)
	assert.IsType(t, &driver.ValueExpr{}, setStmt.Variables[0].Value)
	valueExpr := setStmt.Variables[0].Value.(ast.ValueExpr)
	assert.Equal(t, "CRC32", valueExpr.GetString())

	sql = "SET @slave_uuid='11481c85-e6f2-11e8-8bbe-fa163e72d4ae'"
	sqlParser = parser.New()
	stmt, err = sqlParser.ParseOneStmt(sql, "", "")
	assert.Nil(t, err)
	assert.IsType(t, &ast.SetStmt{}, stmt)

	setStmt = stmt.(*ast.SetStmt)
	valueExpr = setStmt.Variables[0].Value.(ast.ValueExpr)
	slaveUuid := valueExpr.GetString()
	t.Logf("slaveUuid:%s", slaveUuid)
	assert.Equal(t, "11481c85-e6f2-11e8-8bbe-fa163e72d4ae", slaveUuid)

	sql = "SET @master_heartbeat_period=99999997952"
	sqlParser = parser.New()
	stmt, err = sqlParser.ParseOneStmt(sql, "", "")
	assert.Nil(t, err)
	assert.IsType(t, &ast.SetStmt{}, stmt)

	setStmt = stmt.(*ast.SetStmt)
	valueExpr = setStmt.Variables[0].Value.(ast.ValueExpr)
	peroid := valueExpr.GetValue().(int64)/1000000000 + 1
	t.Logf("peroid:%d", peroid)
	assert.Equal(t, int64(100), peroid)

	sql = "SET NAMES latin1"
	sqlParser = parser.New()
	stmt, err = sqlParser.ParseOneStmt(sql, "", "")
	assert.Nil(t, err)
	assert.IsType(t, &ast.SetStmt{}, stmt)

	setStmt = stmt.(*ast.SetStmt)
	valueExpr = setStmt.Variables[0].Value.(ast.ValueExpr)
	assert.Equal(t, "latin1", valueExpr.GetString())
	assert.Equal(t, "setnames", strings.ToLower(setStmt.Variables[0].Name))
}

func TestSelectQuery(t *testing.T) {
	sql := "SELECT @master_binlog_checksum"
	sqlParser := parser.New()
	stmt, err := sqlParser.ParseOneStmt(sql, "", "")
	assert.Nil(t, err)
	assert.IsType(t, stmt, &ast.SelectStmt{})

	selectStmt := stmt.(*ast.SelectStmt)
	assert.Equal(t, 1, len(selectStmt.Fields.Fields))
	assert.IsType(t, &ast.VariableExpr{}, selectStmt.Fields.Fields[0].Expr)
	assert.Equal(t, "master_binlog_checksum", selectStmt.Fields.Fields[0].Expr.(*ast.VariableExpr).Name)

	sql = "SELECT @@global.server_uuid"
	sqlParser = parser.New()
	stmt, err = sqlParser.ParseOneStmt(sql, "", "")
	assert.Nil(t, err)
	assert.IsType(t, stmt, &ast.SelectStmt{})

	selectStmt = stmt.(*ast.SelectStmt)
	assert.Equal(t, 1, len(selectStmt.Fields.Fields))
	assert.IsType(t, &ast.VariableExpr{}, selectStmt.Fields.Fields[0].Expr)
	assert.Equal(t, "server_uuid", selectStmt.Fields.Fields[0].Expr.(*ast.VariableExpr).Name)
}

func DumpBinlogEvent(e *replication.BinlogEvent) error {
	e.Header.Dump(os.Stdout)
	e.Event.Dump(os.Stdout)
	return nil
}
func TestBuildFakeRotate(t *testing.T) {
	binlogParser := replication.NewBinlogParser()

	fde := []byte{78, 238, 198, 91, 15, 78, 12, 0, 0, 119, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 53, 46,
		55, 46, 50, 51, 45, 108, 111, 103, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 19, 56, 13, 0, 8, 0, 18, 0, 4, 4, 4, 4,
		18, 0, 0, 95, 0, 4, 26, 8, 0, 0, 0, 8, 8, 8, 2, 0, 0, 0, 10, 10, 10, 42, 42, 0, 18, 52, 0, 1, 167, 77, 58, 242}

	r1 := bytes.NewReader(fde)
	success, err := binlogParser.ParseSingleEvent(r1, DumpBinlogEvent)
	assert.Nil(t, err)
	assert.Equal(t, false, success)

	data := buildFakeRotateEvent(1, "mysql_bin.000012", true)
	r2 := bytes.NewReader(data)

	success, err = binlogParser.ParseSingleEvent(r2, DumpBinlogEvent)
	assert.Nil(t, err)
	assert.Equal(t, false, success)
}
