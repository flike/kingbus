package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/coreos/bbolt"
	"github.com/flike/kingbus/storage"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

const (
	metadataFile = "kingbus_meta.db"
	bucketName   = "kingbus_meta_bucket"
)

//dump metadata store
func main() {
	dbFilePath := flag.String("db", metadataFile, "kingbus metadata file")
	flag.Parse()

	db, err := bolt.Open(*dbFilePath, 0600, nil)
	if err != nil {
		fmt.Printf("bolt Open error,err=%s,dbpath=%s", err.Error(), *dbFilePath)
		return
	}

	err = db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket not exist")
		}
		b.ForEach(func(k, v []byte) error {
			key := string(k)
			if key == storage.AppliedIndexKey {
				fmt.Printf("%s=%d\n", key, binary.BigEndian.Uint64(v))
			} else if strings.Contains(key, storage.FdePrefix) {
				fmt.Printf("%s value is:\n", key)
				binlogParser := replication.NewBinlogParser()
				r1 := bytes.NewReader(v)
				_, err := binlogParser.ParseSingleEvent(r1, dumpBinlogEvent)
				if err != nil {
					fmt.Printf("ParseSingleEvent error,err:%s", err)
				}
				return err
			} else if strings.Contains(key, storage.PgePrefix) {
				gtids, err := gomysql.DecodeMysqlGTIDSet(v)
				if err != nil {
					return err
				}
				fmt.Printf("%s=%s\n", key, gtids.String())
			} else if strings.Contains(key, storage.ExecutedGtidSetKey) {
				gtids, err := gomysql.DecodeMysqlGTIDSet(v)
				if err != nil {
					return err
				}
				fmt.Printf("%s=%s\n", key, gtids.String())
			} else if strings.Contains(key, storage.GtidPurgedKey) {
				gtids, err := gomysql.DecodeMysqlGTIDSet(v)
				if err != nil {
					return err
				}
				fmt.Printf("%s=%s\n", key, gtids.String())
			} else {
				fmt.Printf("%s=%s\n", k, v)
			}
			return nil
		})
		return nil
	})
	if err != nil {
		fmt.Printf("db view error,err=%s,dbpath=%s", err.Error(), *dbFilePath)
		return
	}
}

func dumpBinlogEvent(e *replication.BinlogEvent) error {
	e.Header.Dump(os.Stdout)
	e.Event.Dump(os.Stdout)
	return nil
}
