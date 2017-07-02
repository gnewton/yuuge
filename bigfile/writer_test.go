package bigfile

import (
	"bytes"
	crand "crypto/rand"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
)

func TestCreate(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var mgr *Manager = nil
	_, err := newWriter(mgr)
	if err == nil {
		t.Log("Should not return nil")
		t.Fail()
	}
}

func TestStart(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	_, _ = makeWriter(t)

}

func TestWriteFiles(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	mgr, _ := makeWriter(t)
	writeRandomFiles(mgr, t)
	log.Println("-----end")
	mgr.Close()
}

func TestWriteReadVerifyFiles(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	mgr, _ := makeWriter(t)
	writeRandomFiles(mgr, t)
	mgr.Close()
}

//////////////// HELPERS ////////////////

func makeWriter(t *testing.T) (*Manager, *FileWriter) {
	mgr, dir, err := makeManager()
	if err != nil || mgr == nil || err != nil {
		t.Log(err)
		t.Fail()
	}
	defer func() {
		os.RemoveAll(dir) // clean up
		//mgr.Close()
	}()

	writer, err := newWriter(mgr)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	return mgr, writer
}

func writeRandomFiles(mgr *Manager, t *testing.T) {
	total := 0
	for i := 0; i < 1000; i++ {
		r, n, err := NewRandomReader()
		if err != nil {
			t.Log(err)
			t.Fail()
		}
		log.Println(n)
		si := StreamInfo{r, "foo"}
		mgr.streamInfo <- &si
		total += n
	}

	log.Println("--------------==========total:", total)
}

func makeManager() (*Manager, string, error) {
	dir, err := ioutil.TempDir("", "bigfile_test")
	if err != nil {
		log.Println(err)
		return nil, "", err
	}
	mgr, err := NewManager(dir, "tmpfile.bf", 1)
	if err != nil {
		log.Println(err)
		return nil, "", err
	}
	return mgr, dir, nil
}

func NewRandomReader() (io.Reader, int, error) {
	n := 4096 + rand.Intn(50001)
	b := make([]byte, n)
	_, err := crand.Read(b)
	if err != nil {
		log.Println(err)
		return nil, -1, err
	}

	return bytes.NewReader(b), n, nil
}
