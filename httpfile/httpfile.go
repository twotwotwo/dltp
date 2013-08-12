package httpfile

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path"          // using / for URLs
	"path/filepath" // using whatever OS wants
)

// Uses a file as a buffer for a remote source.
//
// If the file's already on local disk, it just uses that; otherwise, the
// reader(at) (there has to be just one) waits until the bytes it wants are
// ready.
type HTTPFile struct {
	net            io.Reader // a resource from the World Wide Web (OMG)
	r              *os.File  // what the client is reading from
	w              *os.File  // same file, but this handle is appending
	networkError   error
	readOffset     int64
	newDataWaiting chan bool
	requestedBytes int64
	availableBytes int64
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
		net:            httpResp.Body,
		r:              outreader,
		w:              outwriter,
		newDataWaiting: make(chan bool),
	}

	go hf.download()
	return
}

func (hf *HTTPFile) Read(p []byte) (n int, err error) {
	hf.requestedBytes = hf.readOffset + int64(len(p))
	for (hf.requestedBytes > hf.availableBytes) && <-hf.newDataWaiting {
	}

	n, err = hf.r.Read(p)
	hf.readOffset += int64(n)

	if hf.networkError != nil {
		err = hf.networkError
	}
	return
}

func (hf *HTTPFile) ReadAt(p []byte, offset int64) (n int, err error) {
	hf.requestedBytes = offset + int64(len(p))
	for hf.requestedBytes > hf.availableBytes && <-hf.newDataWaiting {
	}

	n, err = hf.r.ReadAt(p, offset)

	if hf.networkError != nil {
		err = hf.networkError
	}
	return
}

func (hf *HTTPFile) download() {
	copySize := int64(1 << 16)
	for {
		n, err := io.CopyN(hf.w, hf.net, copySize)
		if err != nil {
			if err != io.EOF { // EOF isn't a networkError
				hf.networkError = err
			}
			close(hf.newDataWaiting)
			return
		}
		hf.availableBytes += n
		if hf.requestedBytes <= hf.availableBytes {
			// write, but don't block if no one's listening
			select {
			case hf.newDataWaiting <- true:
			default:
			}
		}
	}
}
