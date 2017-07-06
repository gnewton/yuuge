package bigfile

import (
	"bufio"
	//	"io"
	"log"
	"os"
)

type Writer2 struct {
	writer             *bufio.Writer
	file               *os.File
	filename           string
	offset             int64
	length             int64
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

func NewWriter(outfilenames chan string, segments chan []*Segment, maxFileSize int64) (*Writer2, error) {
	w := Writer2{
		offset:             0,
		length:             0,
		outfilenamesSource: outfilenames,
		segmentsSink:       segments,
		maxFileSize:        maxFileSize,
	}

	err := w.open()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	//w.reset()
	return &w, nil
}

func (w *Writer2) Write(p []byte) (n int, err error) {
	if w.offset+w.length+int64(len(p)) > w.maxFileSize {
		log.Println("BBBBBBBBBBIIIIIIIIIGGGGGGG")
		err := w.close()
		if err != nil {
			log.Println(err)
			return -1, err
		}
		w.sendSegments()

		err = w.open()
		if err != nil {
			log.Println(err)
			return -1, err
		}

		w.reset()

	}

	n, err = w.writer.Write(p)
	if err == nil {
		w.length = w.length + int64(n)
		log.Println("********************", w.length)
	}
	return n, err
}

func (w *Writer2) sendSegments() {
	log.Printf("mmmmm %+v\n", w.segments[0])
	w.segmentsSink <- w.segments
	w.segmentsSink = nil
}

// Call when there is a new file to write
func (w *Writer2) reset() {
	w.segments = make([]*Segment, 0)
	segment := Segment{
		filename: w.filename,
		offset:   w.offset,
		length:   w.length,
	}
	w.segments = append(w.segments, &segment)
}

// Closes the present writer and underlying file
func (w *Writer2) close() error {
	log.Println("===", w.filename, w.offset, w.length)
	segment := Segment{
		filename: w.filename,
		offset:   w.offset,
		length:   w.length,
	}
	w.segments = append(w.segments, &segment)
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
	w.offset = w.offset + w.length
	w.length = 0
	return nil
}

func (w *Writer2) open() error {
	w.filename = <-w.outfilenamesSource
	log.Println("Opening:", w.filename)
	var err error
	w.file, err = os.Create(w.filename)
	if err != nil {
		log.Println(err)
		return err
	}
	w.writer = bufio.NewWriter(w.file)
	return nil
}
