package bigfile

import (
	"io"
	"strconv"
)

type BigFile struct {
	fileCounter  int64
	dir          string
	baseFileName string
	streamInfo   chan *StreamInfo
	index        chan *Index
}

// Info about the stream coming in
type StreamInfo struct {
	r   io.Reader
	key string
}

type Index struct {
	key      string
	offset   int64
	length   int64
	filename string
}

func New(dir string, baseFileName string, numWriters int) (*BigFile, error) {
	bf := BigFile{
		dir:          dir,
		baseFileName: baseFileName,
		fileCounter:  0,
		streamInfo:   make(chan *StreamInfo, numWriters),
		index:        make(chan *Index),
	}

	// init and start FileWriter

	return &bf, nil
}

func (bf *BigFile) Add(r io.Reader, key string) error {
	return nil
}

func (bf *BigFile) NextFileName() string {
	bf.fileCounter += 1
	return bf.baseFileName + "_" + strconv.FormatInt(bf.fileCounter, 10)
}
