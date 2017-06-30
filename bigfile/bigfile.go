package bigfile

import (
	"fmt"
	"io"
	"strconv"
	"sync"
)

//
type Manager struct {
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

func New(dir string, baseFileName string, numWriters int) (*Manager, error) {
	mgr := Manager{
		dir:          dir,
		baseFileName: baseFileName,
		fileCounter:  0,
		streamInfo:   make(chan *StreamInfo, numWriters),
		index:        make(chan *Index),
	}

	// init and start FileWriter

	go func() {
		for i := range mgr.index {
			fmt.Println(i.key, i.offset, i.length)
		}
	}()

	return &mgr, nil
}

func (mgr *Manager) Add(key string, r io.Reader) error {
	return nil
}

var lock sync.Mutex

func (mgr *Manager) NextFileName() string {
	lock.Lock()
	mgr.fileCounter += 1
	val := mgr.baseFileName + "_" + strconv.FormatInt(mgr.fileCounter, 10)
	lock.Unlock()
	return val
}
