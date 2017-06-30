package bigfile

import (
	"bufio"
	"io"
	"log"
	"os"
)

type FileWriter struct {
	mgr    *Manager
	offset int64
}

func NewWriter(mgr *Manager) {
	w := FileWriter{
		mgr: mgr,
	}

	go w.WriteFiles()
}

func (fw *FileWriter) WriteFiles() {
	for {
		filename := fw.mgr.NextFileName()
		fi, err := os.Open(filename)
		if err != nil {
			log.Println(err)
			return
		}
		// close fi on exit and check for its returned error
		defer func() {
			if err := fi.Close(); err != nil {
				log.Println(err)
				return
			}
		}()

		w := bufio.NewWriter(fi)

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
