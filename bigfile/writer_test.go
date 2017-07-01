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
	mgr, dir, err := makeManager()
	if err != nil || mgr == nil || err != nil {
		t.Log(err)
		t.Fail()
	}
	defer func() {
		os.RemoveAll(dir) // clean up
		//mgr.Close()
	}()

	_, err = newWriter(mgr)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
}

func TestWriteFiles(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	mgr, dir, err := makeManager()
	if err != nil || mgr == nil || err != nil {
		t.Log(err)
		t.Fail()
	}
	defer func() {
		os.RemoveAll(dir) // clean up
		if mgr != nil {
			mgr.Close()
		}
	}()

	_, err = newWriter(mgr)
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	total := 0
	for i := 0; i < 10; i++ {
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

	mgr.Close()
	log.Println("total:", total)
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
	n := 4096 + rand.Intn(50000000)
	b := make([]byte, n)
	_, err := crand.Read(b)
	if err != nil {
		log.Println(err)
		return nil, -1, err
	}

	return bytes.NewReader(b), n, nil
}
