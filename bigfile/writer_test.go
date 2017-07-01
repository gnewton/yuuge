package bigfile

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestCreate(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var mgr *Manager = nil
	err := newWriter(mgr)
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

	err = newWriter(mgr)
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

	err = newWriter(mgr)
	if err != nil {
		t.Log(err)
		t.Fail()
	}

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
