package bigfile

import (
	"bufio"
	"io"
	"log"
	"os"
)

type FileWriter struct {
	bf     *BigFile
	offset int64
}

func NewWriter(bf *BigFile) {
	w := FileWriter{
		bf: bf,
	}

	go w.WriteFiles()
}

func (fw *FileWriter) WriteFiles() {
	for {
		filename := fw.bf.NextFileName()
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

		for info := range fw.bf.streamInfo {
			n, err := io.Copy(w, info.r)
			if err != nil {
				log.Println(err)
				return
			}
			index := Index{
				key:      info.key,
				offset:   fw.offset,
				length:   n,
				filename: filename,
			}
			fw.bf.index <- &index
			fw.offset += n
		}

	}
}
