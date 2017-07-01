package bigfile

import (
	"bufio"
	"errors"
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

func newWriter(mgr *Manager) error {
	if mgr == nil {
		return errors.New("Manager is nil")
	}

	w := FileWriter{
		mgr: mgr,
	}

	go w.writeFiles()
	return nil
}

func (fw *FileWriter) close() {
	close(fw.mgr.index)
}

func (fw *FileWriter) writeFiles() {
	for {
		filename := fw.mgr.NextFileName()
		fi, err := os.Open(filename)
		if err != nil {
			log.Println(err)
			return
		}
		fw.openFile = fi
		// close fi on exit and check for its returned error
		defer func() {
			if err := fi.Close(); err != nil {
				log.Println(err)
				return
			}
		}()

		w := bufio.NewWriter(fi)
		fw.openWriter = w

		// Get next stream and write it out to the bigfile
		for info := range fw.mgr.streamInfo {
			n, err := io.Copy(w, info.r)
			if err != nil {
				log.Println(err)
				return
			}
			// Index info (start offset + length)
			index := Index{
				key:      info.key,
				offset:   fw.offset,
				length:   n,
				filename: filename,
			}
			fw.mgr.index <- &index
			fw.offset += n
		}

	}
}
