package bigfile

import (
	"bufio"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"

	"os"
)

type FileWriter struct {
	mgr        *Manager
	offset     int64
	openFile   *os.File
	openWriter io.Writer
}

func newWriter(mgr *Manager) (*FileWriter, error) {
	if mgr == nil {
		return nil, errors.New("Manager is nil")
	}

	w := FileWriter{
		mgr: mgr,
	}

	go w.writeFiles()
	return &w, nil
}

func (fw *FileWriter) close() {

}

func (fw *FileWriter) writeFiles() {
	filename := fw.mgr.NextFileName()
	fi, err := os.Create(filename)
	if err != nil {
		log.Println(err)
		return
	}
	fw.openFile = fi

	w := bufio.NewWriter(fi)
	fw.openWriter = w

	var total int64 = 0
	// Get next stream and write it out to the bigfile
	for info := range fw.mgr.streamInfo {
		h := sha1.New()
		mw := io.MultiWriter(h, w)
		n, err := io.Copy(mw, info.r)

		if err != nil {
			log.Println(err)
			return
		}
		total += n
		//Index info (start offset + length)
		index := Index{
			key:      info.key,
			offset:   fw.offset,
			length:   n,
			filename: filename,
		}
		fw.mgr.index <- &index
		fw.offset += n
		fmt.Printf("% x", h.Sum(nil))
		fmt.Println("-----n:", n)
		fmt.Println("-----total:", total)
	}
	w.Flush()
	fw.openFile.Close()
	fmt.Println("mmmmmm-----total:", total)

	fw.mgr.doneChan <- true
}
