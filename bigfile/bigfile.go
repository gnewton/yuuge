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
	fileCounter   int64
	dir           string
	baseFileName  string
	streamInfo    chan *StreamInfo
	doneChan      chan bool
	indexDoneChan chan bool
	index         chan *Index
	writer        *FileWriter
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
		dir:           dir,
		baseFileName:  baseFileName,
		fileCounter:   0,
		streamInfo:    make(chan *StreamInfo, numWriters),
		index:         make(chan *Index),
		doneChan:      make(chan bool),
		indexDoneChan: make(chan bool),
	}

	go func() {
		log.Println("Start index")
		for i := range mgr.index {
			fmt.Println("index", i.key, i.offset, i.length)
		}
		log.Println("End index")
		mgr.indexDoneChan <- true
	}()

	return &mgr, nil
}

func (mgr *Manager) Close() {
	close(mgr.streamInfo)
	<-mgr.doneChan
	close(mgr.index)
	<-mgr.indexDoneChan

	if mgr.writer != nil {
		log.Println("Manager closing")
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
