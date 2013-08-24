// Public domain, Randall Farmer, 2013

package stream

import (
	"fmt" // likewise
	"io"
	"os" // for error handling
)

/*

STREAM READAT HELPER

take a stream and give it a file-like ReadAt method; we need this to stream
from compressed source files

*/

type Stream interface {
	io.Reader
	io.ReaderAt
	io.Closer
}

type StreamReaderAt struct {
	r io.Reader
	o int64
}

func NewReaderAt(r io.Reader) *StreamReaderAt {
	if r == nil {
		panic("no file in NewReaderAt")
	}
	return &StreamReaderAt{r, 0} // fancy.
}

func (sra *StreamReaderAt) Read(p []byte) (n int, err error) {
	n, err = sra.r.Read(p)
	sra.o += int64(n)
	return
}

var discardBuf []byte

const streamReaderAtDiscardChunk = 1e6

func (sra *StreamReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	bytesToSkip := off - sra.o
	if bytesToSkip < 0 {
		f, ok := sra.r.(*os.File)
		if ok {
			fmt.Fprintln(os.Stderr, "file:", f.Name())
		}
		panic(fmt.Sprint("tried to skip from ", sra.o, " to ", off, " in stream"))
	}
	// would this inefficiently spin if waiting on pipe input?
	// (not actually doing OS pipes here, but curious)
	for bytesToSkip > 0 {
		if discardBuf == nil {
			discardBuf = make([]byte, streamReaderAtDiscardChunk)
		}
		discardInto := discardBuf
		if bytesToSkip < int64(len(discardBuf)) {
			discardInto = discardBuf[:bytesToSkip]
		}
		n, err := sra.Read(discardInto)
		if err != nil {
			fmt.Fprintln(os.Stderr, "OS error while discarding input from stream")
			panic(err)
		}
		bytesToSkip -= int64(n)
	}
	err = error(nil)
	n = 0
	toRead := len(p)
	for n < toRead && err == nil {
		nThisRead := 0
		nThisRead, err = sra.Read(p)
		p = p[nThisRead:]
		n += nThisRead
	}
	return n, err
}

func (sra *StreamReaderAt) Close() error {
	if c, ok := sra.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
