package bigfile

import (
	"log"
	"math"
	"strconv"
	"testing"
)

func TestW2(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	outfilenames := make(chan string)
	segments := make(chan []*Segment, 10)

	go func() {
		var i int64
		for i = 0; i < math.MaxInt64; i++ {
			f := "foo" + strconv.FormatInt(i, 10)
			log.Println("Sending:", f)
			outfilenames <- f
		}
	}()

	go func() {
		for s := range segments {
			log.Println(s)
		}
	}()

	w, err := NewWriter(outfilenames, segments, 1024*1000)

	if err != nil {
		log.Fatal(err)
	}

	n, err := w.Write([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	log.Println(n)

	w.close()
	w.sendSegments()

}
