package server

import (
	"context"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"fmt"

	"github.com/flike/kingbus/config"
	. "github.com/flike/kingbus/log"
	"github.com/flike/kingbus/storage"
	"github.com/flike/kingbus/storage/storagepb"
	"github.com/flike/kingbus/utils"
	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

var leader *KingbusServer
var cluster []*KingbusServer
var totalEvent int

func onEvent(e *replication.BinlogEvent) error {
	newEvent := &storagepb.BinlogEvent{
		Flavor:        storagepb.FlavorType_MySQL,
		Type:          uint32(e.Header.EventType),
		DividedCount:  0,
		DividedSeqNum: 0,
		Data:          e.RawData,
	}
	data, err := utils.EncodeBinlogEvent(newEvent)
	if err != nil {
		return err
	}

	err = leader.raftNode.Propose(context.Background(), data)
	if err != nil {
		return err
	}
	totalEvent++
	return nil
}

//1.start kingbus
//2.parse binlog file
//3.propose binlog event into raft cluster
func InitClusterForTest(t *testing.T) {
	totalEvent = 0
	cluster = StartClusterInTest()

	//propose master gtidPurged
	masterGtidPurged := config.MasterGtidPurged{GtidPurged: "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-11789"}
	data, err := masterGtidPurged.EncodeWithType()
	require.Nil(t, err)
	err = leader.raftNode.Propose(context.Background(), data)
	require.Nil(t, err)

	//get binlog file name
	binlogFileDir := "../tools/test_data/binlog/"
	files, err := utils.ReadDir(binlogFileDir)
	require.Nil(t, err)

	logFiles := make([]os.FileInfo, 0, 3)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mysql_bin") {
			logFiles = append(logFiles, file)
		}
	}
	require.Equal(t, len(logFiles), 3)

	//pase binlog file,
	p := replication.NewBinlogParser()
	for i := 0; i < len(logFiles); i++ {
		name := path.Join(binlogFileDir, logFiles[i].Name())
		err = p.ParseFile(name, 0, onEvent)
		require.Nil(t, err)
	}
	//wait for apply
	time.Sleep(3 * time.Second)
}

func startBinlogServerForTest(t *testing.T) {
	binlogMasterArgs := &config.BinlogServerConfig{
		Addr:     "127.0.0.1:9797",
		User:     "kingbus",
		Password: "123456",
	}
	Log.Infof("after sleep 3s in InitClusterForTest")
	err := leader.StartServer(config.BinlogServerType, binlogMasterArgs)
	Log.Infof("leader applyIndex:%d", leader.getAppliedIndex())
	require.Nil(t, err)
}

func StopBinlogServerForTest(t *testing.T) {
	StopClusterInTest(cluster)
}

//test dump at the head of mysql_bin000012
func TestBinlogServer_BinlogDumpGtid2(t *testing.T) {
	InitClusterForTest(t)
	defer StopBinlogServerForTest(t)
	startBinlogServerForTest(t)

	syncerArgs := &config.SyncerArgs{
		SyncerID:      10,
		SynerUUID:     "6f90c875-ee30-11e8-8bbe-fa163e72d4ae",
		MysqlAddr:     "127.0.0.1:9797",
		MysqlUser:     "kingbus",
		MysqlPassword: "123456",
		SemiSync:      false,
	}

	//new storage
	store, err := storage.NewDiskStorage(TestSyncerDir, 4)
	require.Nil(t, err)
	defer store.Close()

	//New Syncer Config
	syncerCfg, err := config.NewSyncerConfig(syncerArgs)
	require.Nil(t, err)

	//New Syncer
	syncer, err := NewSyncer(syncerCfg, store)
	require.Nil(t, err)

	//start syncer
	res, err := syncer.Execute("SELECT @@gtid_mode")
	require.Nil(t, err)
	mode, _ := res.GetString(0, 0)
	require.Equal(t, mode, "ON")

	//check the row mode in master
	res, err = syncer.Execute(`SELECT @@global.binlog_format`)
	require.Nil(t, err)
	f, _ := res.GetString(0, 0)
	require.Equal(t, f, "ROW")

	//save the master info, using in binlog server
	err = syncer.GetMasterInfo()
	require.Nil(t, err)

	gtids, err := gomysql.ParseGTIDSet("mysql", "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-11800")
	require.Nil(t, err)
	//start syncer
	binlogStreamer, err := syncer.io.StartSyncGTID(gtids)
	require.Nil(t, err)

	//binlog file total
	var trxCount int
	for {
		event, err := binlogStreamer.GetEvent(syncer.ctx)
		require.Nil(t, err)
		if event.Header.EventType == replication.GTID_EVENT {
			//first trx
			if trxCount == 0 {
				firstGtid := getGtidFromGtidEvent(event.RawData)
				require.Equal(t, "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:11801", firstGtid)
			}
			//last trx
			if trxCount == 19 {
				lastGtid := getGtidFromGtidEvent(event.RawData)
				require.Equal(t, "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:11820", lastGtid)
				break
			}
			fmt.Printf("trxCount:%d,gtid:%s\n", trxCount, getGtidFromGtidEvent(event.RawData))
			trxCount++
		}
	}
}

//test dump binlog event by syncer
//test dump at the middle of mysql_bin000012
func TestBinlogServer_BinlogDumpGtid3(t *testing.T) {
	InitClusterForTest(t)
	defer StopBinlogServerForTest(t)
	startBinlogServerForTest(t)

	syncerArgs := &config.SyncerArgs{
		SyncerID:      10,
		SynerUUID:     "6f90c875-ee30-11e8-8bbe-fa163e72d4ae",
		MysqlAddr:     "127.0.0.1:9797",
		MysqlUser:     "kingbus",
		MysqlPassword: "123456",
		SemiSync:      false,
	}

	//new storage
	store, err := storage.NewDiskStorage(TestSyncerDir, 4)
	require.Nil(t, err)
	defer store.Close()

	//New Syncer Config
	syncerCfg, err := config.NewSyncerConfig(syncerArgs)
	require.Nil(t, err)

	//New Syncer
	syncer, err := NewSyncer(syncerCfg, store)
	require.Nil(t, err)

	//start syncer
	res, err := syncer.Execute("SELECT @@gtid_mode")
	require.Nil(t, err)
	mode, _ := res.GetString(0, 0)
	require.Equal(t, mode, "ON")

	//check the row mode in master
	res, err = syncer.Execute(`SELECT @@global.binlog_format`)
	require.Nil(t, err)
	f, _ := res.GetString(0, 0)
	require.Equal(t, f, "ROW")

	//save the master info, using in binlog server
	err = syncer.GetMasterInfo()
	require.Nil(t, err)

	gtids, err := gomysql.ParseGTIDSet("mysql", "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-11802")
	require.Nil(t, err)
	//start syncer
	binlogStreamer, err := syncer.io.StartSyncGTID(gtids)
	require.Nil(t, err)

	//binlog file total
	var trxCount int
	for {
		event, err := binlogStreamer.GetEvent(syncer.ctx)
		require.Nil(t, err)

		if event.Header.EventType == replication.GTID_EVENT {
			//first trx
			if trxCount == 0 {
				firstGtid := getGtidFromGtidEvent(event.RawData)
				require.Equal(t, "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:11803", firstGtid)
			}
			//last trx
			if trxCount == 17 {
				lastGtid := getGtidFromGtidEvent(event.RawData)
				require.Equal(t, "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:11820", lastGtid)
				break
			}
			trxCount++
		}
	}
}

func TestBinlogServer_HeartbeatEvent(t *testing.T) {
	InitClusterForTest(t)
	defer StopBinlogServerForTest(t)
	startBinlogServerForTest(t)

	syncerArgs := &config.SyncerArgs{
		SyncerID:      10,
		SynerUUID:     "6f90c875-ee30-11e8-8bbe-fa163e72d4ae",
		MysqlAddr:     "127.0.0.1:9797",
		MysqlUser:     "kingbus",
		MysqlPassword: "123456",
		SemiSync:      false,
	}

	//new storage
	store, err := storage.NewDiskStorage(TestSyncerDir, 4)
	require.Nil(t, err)
	defer store.Close()

	//New Syncer Config
	syncerCfg, err := config.NewSyncerConfig(syncerArgs)
	require.Nil(t, err)

	//New Syncer
	syncer, err := NewSyncer(syncerCfg, store)
	require.Nil(t, err)

	//start syncer
	res, err := syncer.Execute("SELECT @@gtid_mode")
	require.Nil(t, err)
	mode, _ := res.GetString(0, 0)
	require.Equal(t, mode, "ON")

	//check the row mode in master
	res, err = syncer.Execute(`SELECT @@global.binlog_format`)
	require.Nil(t, err)
	f, _ := res.GetString(0, 0)
	require.Equal(t, f, "ROW")

	//save the master info, using in binlog server
	err = syncer.GetMasterInfo()
	require.Nil(t, err)

	gtids, err := gomysql.ParseGTIDSet("mysql", "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-11812")
	require.Nil(t, err)
	//start syncer
	binlogStreamer, err := syncer.io.StartSyncGTID(gtids)
	require.Nil(t, err)

	//binlog file total
	var trxCount int
	for {
		event, err := binlogStreamer.GetEvent(syncer.ctx)
		require.Nil(t, err)

		if event.Header.EventType == replication.GTID_EVENT {
			//first trx
			if trxCount == 0 {
				firstGtid := getGtidFromGtidEvent(event.RawData)
				require.Equal(t, "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:11813", firstGtid)
			}
			//last trx
			if trxCount == 7 {
				lastGtid := getGtidFromGtidEvent(event.RawData)
				require.Equal(t, "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:11820", lastGtid)
			}
			trxCount++
		}
		if event.Header.EventType == replication.HEARTBEAT_EVENT {
			dumpHeartbeat(event.RawData)
			break
		}
	}
}

//test dump binlog event by syncer
//test dump event which size large than 16MB
func TestBinlogServer_BinlogDumpGtid4(t *testing.T) {
	//1.start binlog server
	//2.mockProposeBinlogEventForTest
	//3.start
}

type MockKingbusInfoInTest struct {
}

func (m *MockKingbusInfoInTest) AppliedIndex() uint64 {
	return 0
}

func (m *MockKingbusInfoInTest) LastBinlogFile() string {
	return ""
}

func (m *MockKingbusInfoInTest) LastFilePosition() uint32 {
	return 0
}

func (m *MockKingbusInfoInTest) ExecutedGtidSetStr() string {
	return "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-7"
}

func TestBinlogServer_CheckGtidSet(t *testing.T) {
	store, err := storage.NewDiskStorage(TestSyncerDir, 4)
	require.Nil(t, err)
	defer store.Close()

	binlogMaster := new(BinlogServer)
	binlogMaster.store = store
	binlogMaster.kingbusInfo = &MockKingbusInfoInTest{}

	masterPurgedGtid, err := gomysql.ParseGTIDSet("mysql", "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-3")
	require.Nil(t, err)
	store.SetGtidSet("mysql", storage.GtidPurgedKey, masterPurgedGtid)

	slaveExecutedGtid, err := gomysql.ParseGTIDSet("mysql",
		"214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-7,b5cb9c98-de7c-11e8-9c84-fa163e8d0f56:2-8")
	require.Nil(t, err)
	err = binlogMaster.CheckGtidSet("mysql", slaveExecutedGtid)
	require.Nil(t, err)

	slaveExecutedGtid, err = gomysql.ParseGTIDSet("mysql",
		"214eb71f-96ec-11e8-ab4f-fa163e72d4ae:2-8,b5cb9c98-de7c-11e8-9c84-fa163e8d0f56:2-8")
	require.Nil(t, err)
	err = binlogMaster.CheckGtidSet("mysql", slaveExecutedGtid)
	require.Equal(t, ErrNotContain, err)
}

//test start a syncer
func TestBinlogServer_StartSyncer(t *testing.T) {
	InitClusterForTest(t)
	defer StopBinlogServerForTest(t)
	startBinlogServerForTest(t)

	syncerArgs := &config.SyncerArgs{
		SyncerID:      10,
		SynerUUID:     "6f90c875-ee30-11e8-8bbe-fa163e72d4ae",
		MysqlAddr:     "127.0.0.1:9797",
		MysqlUser:     "kingbus",
		MysqlPassword: "123456",
		SemiSync:      false,
	}

	//new storage
	store, err := storage.NewDiskStorage(TestSyncerDir, 4)
	require.Nil(t, err)
	defer store.Close()

	//New Syncer Config
	syncerCfg, err := config.NewSyncerConfig(syncerArgs)
	require.Nil(t, err)

	//New Syncer
	syncer, err := NewSyncer(syncerCfg, store)
	require.Nil(t, err)

	//start syncer
	res, err := syncer.Execute("SELECT @@gtid_mode")
	require.Nil(t, err)
	mode, _ := res.GetString(0, 0)
	require.Equal(t, mode, "ON")

	//check the row mode in master
	res, err = syncer.Execute(`SELECT @@global.binlog_format`)
	require.Nil(t, err)
	f, _ := res.GetString(0, 0)
	require.Equal(t, f, "ROW")

	//save the master info, using in binlog server
	err = syncer.GetMasterInfo()
	require.Nil(t, err)

	gtids, err := gomysql.ParseGTIDSet("mysql", "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-11789")
	require.Nil(t, err)
	//start syncer
	binlogStreamer, err := syncer.io.StartSyncGTID(gtids)
	require.Nil(t, err)

	//binlog file total
	var trxCount int
	for {
		event, err := binlogStreamer.GetEvent(syncer.ctx)
		require.Nil(t, err)
		if event.Header.EventType == replication.GTID_EVENT {
			currentGtid := getGtidFromGtidEvent(event.RawData)
			t.Logf("trxCount:%d,gtid:%s\n", trxCount, currentGtid)
			//first trx
			if trxCount == 0 {
				firstGtid := getGtidFromGtidEvent(event.RawData)
				require.Equal(t, "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:11790", firstGtid)
			}
			//last trx
			if trxCount == 30 {
				lastGtid := getGtidFromGtidEvent(event.RawData)
				require.Equal(t, "214eb71f-96ec-11e8-ab4f-fa163e72d4ae:11820", lastGtid)
				break
			}
			trxCount++
		}
	}
}

func getGtidFromGtidEvent(data []byte) string {
	//remove event header, parse event body, and save
	data = data[replication.EventHeaderSize:]
	//remove crc32
	data = data[:len(data)-replication.BinlogChecksumLength]
	e := &replication.GTIDEvent{}
	if err := e.Decode(data); err != nil {
		Log.Fatalf("Decode GtidEvent error,err:%s,eventRawData:%v", err, data)
	}
	u, err := uuid.FromBytes(e.SID)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s:%d", u.String(), e.GNO)
}

func dumpHeartbeat(data []byte) {
	h := &replication.EventHeader{}
	if err := h.Decode(data); err != nil {
		Log.Fatalf("Decode heartbeat header error,err:%s,eventRawData:%v", err, data)
	}
	h.Dump(os.Stdout)
	data = data[replication.EventHeaderSize:]
	//remove crc32
	data = data[:len(data)-replication.BinlogChecksumLength]
	fmt.Printf("lastBinlog:%s\n", string(data))
}

//SELECT @@GLOBAL.SERVER_UUID
//SELECT @@GLOBAL.SERVER_ID
//SELECT UNIX_TIMESTAMP()
//SELECT @@gtid_mode
//SELECT @@gtid_purged
//SELECT @@global.binlog_checksum
//SELECT @@global.binlog_format
//SELECT @master_binlog_checksum

//SET @master_binlog_checksum='CRC32'
//SET @master_heartbeat_period=10000000000;
//SET @slave_uuid='34287d42-dc29-11e8-8bbe-fa163e72d4ae'

//test the binlog server supported query
func TestBinlogServer_HandleQuery(t *testing.T) {
	InitClusterForTest(t)
	defer StopBinlogServerForTest(t)
	startBinlogServerForTest(t)

	syncerArgs := &config.SyncerArgs{
		SyncerID:      10,
		SynerUUID:     "6f90c875-ee30-11e8-8bbe-fa163e72d4ae",
		MysqlAddr:     "127.0.0.1:9797",
		MysqlUser:     "kingbus",
		MysqlPassword: "123456",
		SemiSync:      false,
	}

	//new storage
	store, err := storage.NewDiskStorage(TestSyncerDir, 4)
	require.Nil(t, err)
	defer store.Close()

	//New Syncer Config
	syncerCfg, err := config.NewSyncerConfig(syncerArgs)
	require.Nil(t, err)

	//New Syncer
	syncer, err := NewSyncer(syncerCfg, store)
	require.Nil(t, err)

	res, err := syncer.Execute("SELECT @@global.SERVER_UUID")
	require.Nil(t, err)
	serverUuuid, err := res.GetString(0, 0)
	require.Nil(t, err)
	t.Logf("serverUuuid:%s", serverUuuid)

	res, err = syncer.Execute("SELECT @@global.SERVER_ID")
	require.Nil(t, err)
	serverID, _ := res.GetInt(0, 0)
	t.Logf("serverID:%d", serverID)

	res, err = syncer.Execute("SELECT UNIX_TIMESTAMP()")
	require.Nil(t, err)
	tm, err := res.GetString(0, 0)
	require.Nil(t, err)
	t.Logf("tm:%s", tm)

	//start syncer
	res, err = syncer.Execute("SELECT @@gtid_mode")
	require.Nil(t, err)
	mode, err := res.GetString(0, 0)
	require.Nil(t, err)
	require.Equal(t, mode, "ON")

	res, err = syncer.Execute("SELECT @@gtid_purged")
	require.Nil(t, err)
	gtidPurged, err := res.GetString(0, 0)
	require.Nil(t, err)
	t.Logf("gtidPurged:%s", gtidPurged)

	//check the row mode in master
	res, err = syncer.Execute(`SELECT @@global.binlog_format`)
	require.Nil(t, err)
	f, err := res.GetString(0, 0)
	require.Nil(t, err)
	require.Equal(t, f, "ROW")

	res, err = syncer.Execute(`SELECT @@global.binlog_checksum`)
	require.Nil(t, err)
	bc, err := res.GetString(0, 0)
	require.Nil(t, err)
	require.Equal(t, bc, "CRC32")

	res, err = syncer.Execute(`SET @master_binlog_checksum='CRC32'`)
	require.Nil(t, err)

	res, err = syncer.Execute(`SELECT @master_binlog_checksum`)
	require.Nil(t, err)
	bc, err = res.GetString(0, 0)
	require.Nil(t, err)
	require.Equal(t, bc, "CRC32")

	res, err = syncer.Execute(`SET @master_heartbeat_period=100000;`)
	require.Nil(t, err)

	res, err = syncer.Execute(`SET @slave_uuid='34287d42-dc29-11e8-8bbe-fa163e72d4ae'`)
	require.Nil(t, err)
}
