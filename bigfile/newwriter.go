package bigfile

import (
	"bufio"
	"io"
	"log"
	"os"
)

type Writer2 struct {
	writer             *bufio.Writer
	file               *os.File
	filename           string
	offset             int64
	length             int64
	dir                string
	outfilenamesSource chan string
	segmentsSink       chan []*Segment
	maxFileSize        int64
	segments           []*Segment
}

type Segment struct {
	filename string
	offset   int64
	length   int64
}

func NewWriter(dir string, outfilenames chan string, segments chan []*Segment, maxFileSize int64) (io.Writer, error) {
	w := Writer2{
		offset:             0,
		length:             0,
		dir:                dir,
		outfilenamesSource: outfilenames,
		segmentsSink:       segments,
		maxFileSize:        maxFileSize,
	}

	err := w.open()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	w.reset()
	return w, nil
}

func (w Writer2) Write(p []byte) (n int, err error) {
	if w.offset+int64(len(p)) > w.maxFileSize {
		w.close()
		w.open()
		w.sendSegments()
		w.reset()

	}

	n, err = w.writer.Write(p)
	if err == nil {
		w.offset = w.offset + int64(n)
	}
	return n, err
}

func (w *Writer2) sendSegments() {
	w.segmentsSink <- w.segments
	w.segmentsSink = nil
}

// Call when there is a new file to write
func (w *Writer2) reset() {
	w.segments = make([]*Segment, 1)
	segment := Segment{
		filename: w.filename,
		offset:   w.offset,
		length:   w.length,
	}
	w.segments = append(w.segments, &segment)
}

// Closes the present writer and underlying file
func (w *Writer2) close() error {
	err := w.writer.Flush()
	if err != nil {
		log.Println(err)
		return err
	}
	err = w.file.Sync()
	if err != nil {
		log.Println(err)
		return err
	}
	err = w.file.Close()
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (w *Writer2) open() error {
	filename := <-w.outfilenamesSource
	fullpath := w.dir + string(os.PathSeparator) + filename

	var err error
	w.file, err = os.Create(fullpath)
	if err != nil {
		log.Println(err)
		return err
	}
	w.writer = bufio.NewWriter(w.file)
	return nil
}
