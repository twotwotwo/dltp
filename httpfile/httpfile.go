package httpfile

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"          // using / for URLs
	"path/filepath" // using whatever OS wants
)

// Uses a file to save a remote source. As the file downloads you can Read or ReadAt
// on it; if you ask for bytes that aren't downloaded yet, you'll block 'til they're
// ready.
//
// Quirks: no resume, no TLS, always saves with filename from end of URL (thus *can't*
// download http://www.google.com/), probably unnecessarily fussy implementation.
type HTTPFile struct {
	net           io.Reader // a resource from the World Wide Web (OMG)
	r             *os.File  // what the client is reading from
	w             *os.File  // same file, but this handle is appending
	networkError  error
	readOffs      int64
	availableOffs int64

	// you may be waiting from multiple goroutines. we're on it
	waiters    []ByteWaiter
	newWaiters chan ByteWaiter
	closing    chan bool // done reading
	done       chan bool // done downloading
}

type ByteWaiter struct {
	requestedOffs int64
	reply         chan bool
}

var ErrNoUsefulFilename = errors.New("couldn't get filename from URL")

func Open(url string, workingDir *os.File) (hf *HTTPFile, err error) {
	fn := path.Base(url)
	if fn == "." || fn == "/" || fn == "" {
		return nil, ErrNoUsefulFilename
	}
	path := filepath.Join(workingDir.Name(), fn)

	httpResp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	if httpResp.StatusCode != 200 {
		return nil, errors.New("Bad HTTP status: " + httpResp.Status)
	}

	// doesn't support resume yet -- for that, open, check length, req range
	outwriter, err := os.Create(path)
	if err != nil {
		return nil, errors.New("could not open/create outfile:" + err.Error())
	}

	// independent reader
	outreader, err := os.Open(path)
	if err != nil {
		return nil, errors.New("could not read outfile:" + err.Error())
	}

	hf = &HTTPFile{
		net:        httpResp.Body,
		r:          outreader,
		w:          outwriter,
		newWaiters: make(chan ByteWaiter),
		closing:    make(chan bool),
		done:       make(chan bool),
	}

	go hf.download()
	return
}

func (hf *HTTPFile) Read(p []byte) (n int, err error) {
	offs := hf.readOffs + int64(len(p))
	hf.waitForOffs(offs)

	n, err = hf.r.Read(p)
	hf.readOffs += int64(n)

	if hf.networkError != nil {
		err = hf.networkError
	}
	return
}

func (hf *HTTPFile) ReadAt(p []byte, offset int64) (n int, err error) {
	offs := offset + int64(len(p))
	hf.waitForOffs(offs)

	n, err = hf.r.ReadAt(p, offset)

	if hf.networkError != nil {
		err = hf.networkError
	}
	return
}

func (hf *HTTPFile) Close() error {
	close(hf.closing)
	return nil
}

func (hf *HTTPFile) waitForOffs(offs int64) {
	if offs <= hf.availableOffs {
		return
	}
	replyChan := make(chan bool)
	hf.newWaiters <- ByteWaiter{offs, replyChan}
	// we don't bother returning whether or not we reached the offset; the Read/ReadAt
	// call on the underlying file will do the right thing either way
	<-replyChan
}

func (hf *HTTPFile) download() {
	copySize := int64(1 << 16)
	progress := make(chan int64)
	go hf.notify(progress)
	for {
		n, err := io.CopyN(hf.w, hf.net, copySize)
		progress <- n
		if err != nil {
			if err != io.EOF { // EOF isn't a networkError
				hf.networkError = err
			}
			hf.done <- true
			return
		}
	}
}

func (hf *HTTPFile) notify(progress chan int64) {
	done := false
	for {
		select {
		case w := <-hf.newWaiters:
			if done {
				w.reply <- false
			} else if w.requestedOffs <= hf.availableOffs {
				// their thread saw an old offset (smells unlikely, but also seems
				// technically allowed by the memory model)
				w.reply <- true
			} else {
				hf.waiters = append(hf.waiters, w)
			}
		case n := <-progress:
			// OK, progress has happened; tell everyone that can
			// now read, and take them off the waiting list.
			j := 0
			hf.availableOffs += n
			for i := 0; i < len(hf.waiters); i++ {
				w := hf.waiters[i]
				if w.requestedOffs <= hf.availableOffs {
					w.reply <- true
					// remove it from the list (may be overwritten below)
					hf.waiters[i] = ByteWaiter{}
					continue
				} else {
					hf.waiters[j] = hf.waiters[i]
					j++
				}
			}
			hf.waiters = hf.waiters[:j]
		case <-hf.done:
			// done appending; tell everyone and empty out waiters
			if done {
				fmt.Println("uh oh got done twice")
			} else {
				done = true
				for _, w := range hf.waiters {
					close(w.reply)
				}
				hf.waiters = hf.waiters[:0]
			}
		case <-hf.closing:
			return
		}
	}
}
