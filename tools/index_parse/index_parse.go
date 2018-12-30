package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/flike/kingbus/storage"
)

//dump metadata store
func main() {
	indexFilePath := flag.String("index", "./00000000000000000001-inprogress.index", "kingbus storage index file")
	position := flag.Int("pos", 0, "start position of parse file")
	flag.Parse()

	file, err := os.OpenFile(*indexFilePath, os.O_RDWR, 0600)
	if err != nil {
		fmt.Printf("OpenFile error,err:%s,filePath:%s", err, *indexFilePath)
		return
	}

	var pos = int64(*position)
	buf := make([]byte, storage.IndexEntrySize)
	for {
		n, err := file.ReadAt(buf, pos)
		if err != nil {
			if err == io.EOF {
				if n == 0 {
					break
				} else {
					fmt.Printf("index file[%s] torn write,pos:%d,buf:%v,n:%d\n", *indexFilePath, pos, buf, n)
					return
				}
			}
			fmt.Printf("index file[%s]  ReadAt error,err:%s,pos:%d\n", *indexFilePath, err, pos)
			return
		}
		indexEntry := new(storage.IndexEntry)
		err = indexEntry.Unmarshal(buf)
		if err != nil {
			fmt.Printf("Unmarshal buf error,err:%s,buf:%v,n:%d,pos:%d\n", err, buf, n, pos)
			pos += int64(storage.IndexEntrySize)
			continue
		}
		pos += int64(storage.IndexEntrySize)
		fmt.Printf("raftIndex:%d,filePosition:%d\n", indexEntry.RaftIndex, indexEntry.FilePosition)
	}
}
