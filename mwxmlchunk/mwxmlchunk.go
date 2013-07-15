// Public domain, Randall Farmer, 2013

package mwxmlchunk

import (
    "io"
    sref "github.com/twotwotwo/dltp/sourceref"
    "github.com/twotwotwo/dltp/alloc"
    "github.com/twotwotwo/dltp/scan"
)

/* WALKING THROUGH PAGES 

ReadNext() -> text, key, sourceRef, err
  gives you an article or error

ReadTo(key) -> [same]
  reads 'til you reach a key (or pass over it, or reach EOF)

*/

type SegmentKey int
var StartKey SegmentKey = 0
var PastEndKey SegmentKey = (1<<63)-1
var BeforeStart SegmentKey = -PastEndKey

var pageTag []byte = []byte("<page>")
var closePageTag []byte = []byte("</page>")
var nsTag []byte = []byte("<ns>")
var idTag []byte = []byte("<id>")
var revTag []byte = []byte("<revision>")

type SegmentReader struct {
    in *scan.Scanner
    currentSeg []byte
    nextKey SegmentKey
    currentKey SegmentKey
    offs int64
    sourceNumber int64
}

func NewSegmentReader(f io.Reader, sourceNumber int64) (s *SegmentReader) {
    s = &SegmentReader{in: scan.NewScanner(f, 1e6), sourceNumber: sourceNumber, currentKey: BeforeStart}
    s.currentSeg = make([]byte, 0, 1e6)
    return
}

func (s *SegmentReader) ReadNext() (text []byte, key SegmentKey, sr sref.SourceRef, err error) {
    startOffs := s.in.Offs
    var endOffs int64
    if s.nextKey == PastEndKey { // EOF--stop at NOTHING
      endOffs = -1
    } else if s.nextKey == StartKey { // start of file--stop before <page>
      endOffs = s.in.ScanTo(pageTag, false, false)
    } else { // normal--stop after </page>
      endOffs = s.in.ScanTo(closePageTag, true, false)
    }

    s.currentKey = s.nextKey

    if endOffs == -1 {
      s.nextKey = PastEndKey
      endOffs = startOffs + int64(len(s.in.All))
      err = io.EOF
    }

    s.currentSeg = alloc.CopyBytes(s.currentSeg, s.in.All[:endOffs-startOffs])
    //s.currentSeg = alloc.CopyBytes(s.currentSeg, s.in.Content())
    s.in.Discard()
    
    text = s.currentSeg
    key = s.currentKey
    sr = sref.SourceRef{s.sourceNumber, uint64(startOffs), uint64(len(s.currentSeg))}

    // get next id; set EOF flag if we have to
    idTagOffs := s.in.ScanTo(idTag, true, false)
    s.nextKey = SegmentKey(s.in.PeekInt())
    if idTagOffs == -1 {
        s.nextKey = PastEndKey
    }
    /*
    if s.nextKey < s.currentKey {
      fmt.Fprintln(os.Stderr, "warning: id", s.nextKey, "<", s.currentKey, "in source", s.sourceNumber)
    }
    */

    return
}

func (s *SegmentReader) ReadTo(key SegmentKey) (text []byte, reachedKey SegmentKey, sr sref.SourceRef, err error) {
    sr = sref.SourceNotFound
    reachedKey = s.currentKey
    for reachedKey < key {
      text, reachedKey, sr, err = s.ReadNext()
      if err != nil {
        break
      }
    }
    if reachedKey == key { // success! pretty much
      return
    }

    // failed to find! which could happen at EOF, or earlier if incr includes
    // a page ID that was skipped over in the reference (which should be rare, 
    // but we'll handle)
    text = nil
    sr = sref.SourceNotFound
    return
}


