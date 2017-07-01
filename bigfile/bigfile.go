package bigfile

import (
	"errors"
	"fmt"
	"io"
	"log"
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
	writer       *FileWriter
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

func NewManager(dir string, baseFileName string, numWriters int) (*Manager, error) {

	if len(dir) == 0 || len(baseFileName) == 0 || numWriters < 1 {
		return nil, errors.New("dir" + dir + " or baseFilename are nil:" + baseFileName + " or numWriters is < 1")
	}

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

func (mgr *Manager) Close() {
	close(mgr.streamInfo)
	if mgr.writer != nil {
		mgr.writer.close()
	}
}

func (mgr *Manager) Add(key []byte, r io.Reader) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}
	if len(key) == 0 {
		return errors.New("key cannot have zero length")
	}
	return nil
}

var lock sync.Mutex

func (mgr *Manager) NextFileName() string {
	lock.Lock()
	mgr.fileCounter += 1
	val := mgr.baseFileName + "_" + strconv.FormatInt(mgr.fileCounter, 10)
	log.Println("New file", val)
	lock.Unlock()
	return val
}
