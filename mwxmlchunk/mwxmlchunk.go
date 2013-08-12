// Public domain, Randall Farmer, 2013

package mwxmlchunk

import (
	//"github.com/twotwotwo/dltp/alloc"
	"bytes"
	"github.com/twotwotwo/dltp/scan"
	sref "github.com/twotwotwo/dltp/sourceref"
	"io"
)

/* WALKING THROUGH PAGES

ReadNext() -> text, key, sourceRef, err
  gives you an article or error

ReadTo(key) -> [same]
  reads 'til you reach a key (or pass over it, or reach EOF)

*/

type SegmentKey int64

var StartKey SegmentKey = 0
var PastEndKey SegmentKey = (1 << 63) - 1
var BeforeStart SegmentKey = -PastEndKey

var pageTag []byte = []byte("<page>")
var closePageTag []byte = []byte("</page>")
var nsTag []byte = []byte("<ns>")
var idTag []byte = []byte("<id>")
var revTag []byte = []byte("<revision>")
var revOrClosePageTags [][]byte = [][]byte{revTag, closePageTag}

type SegmentReader struct {
	in           *scan.Scanner
	currentSeg   []byte
	backingSeg   []byte
	nextKey      SegmentKey
	currentKey   SegmentKey
	offs         int64
	sourceNumber int64
	lastRevOnly  bool
	limitToNS    bool
	ns           int
	cutMeta      bool
}

func NewSegmentReader(f io.Reader, sourceNumber int64, lastRevOnly bool, limitToNS bool, ns int, cutMeta bool) (s *SegmentReader) {
	s = &SegmentReader{
		in:           scan.NewScanner(f, 1e6),
		sourceNumber: sourceNumber,
		currentKey:   BeforeStart,
		lastRevOnly:  lastRevOnly,
		limitToNS:    limitToNS,
		ns:           ns,
		cutMeta:      cutMeta,
	}
	s.currentSeg = make([]byte, 0, 1e6)
	return
}

func (s *SegmentReader) ReadNext() (text []byte, key SegmentKey, sr sref.SourceRef, err error) {
	startOffs := s.in.Offs
	s.currentSeg = s.backingSeg[:0]
	tag := []byte(nil)
	var endOffs int64
	if s.nextKey == PastEndKey { // EOF--stop at NOTHING
		endOffs = -1
	} else if s.nextKey == StartKey { // start of file--stop before <page>
		endOffs = s.in.ScanTo(pageTag, false, false)
	} else { // normal--stop after </page>
		if s.lastRevOnly {
			// we've only read up to id -- find either <revision> or </page>
			endOffs, tag = s.in.ScanToAny(revOrClosePageTags, true, false)
			if endOffs == -1 {
				// not expected, but recoverable: file truncated in page metadata
			} else {
				// save the metadata
				s.currentSeg = append(
					s.currentSeg,
					s.in.All[:endOffs-startOffs]...,
				)
				// keep reading and discarding revisions, until we hit </page>
				for tag != nil && &tag[0] == &revTag[0] {
					s.in.Discard()
					startOffs = s.in.Offs
					endOffs, tag = s.in.ScanToAny(revOrClosePageTags, true, false)
				}
			}
		} else {
			endOffs = s.in.ScanTo(closePageTag, true, false)
		}
	}

	s.currentKey = s.nextKey

	if endOffs == -1 {
		s.nextKey = PastEndKey
		endOffs = startOffs + int64(len(s.in.All))
		err = io.EOF
	}

	s.currentSeg = append(s.currentSeg, s.in.All[:endOffs-startOffs]...)
	// cutMeta will return a slice in the middle of the segment, so save the
	// true start of it to reuse later.
	s.backingSeg = s.currentSeg
	if s.cutMeta {
		s.currentSeg = cutMeta(s.currentSeg)
	}
	s.in.Discard()

	text = s.currentSeg
	key = s.currentKey
	if s.lastRevOnly {
		// the text we return doesn't correspond to any input
		sr = sref.InvalidSource
	} else {
		sr = sref.SourceRef{s.sourceNumber, uint64(startOffs), uint64(len(s.currentSeg))}
	}

	// get next ns, skipping page it's not "ours"
	if s.limitToNS {
		for {
			nsTagOffs := s.in.ScanTo(nsTag, true, false)
			if nsTagOffs == -1 {
				s.nextKey = PastEndKey
				return
			}
			ns := s.in.PeekInt()
			if ns == s.ns {
				break
			}
			// cleanly discard this page and go on
			s.in.ScanTo(closePageTag, true, false)
			s.in.Discard()
		}
	}

	// get next id; set EOF flag if we have to
	idTagOffs := s.in.ScanTo(idTag, true, false)
	s.nextKey = SegmentKey(s.in.PeekInt())
	if idTagOffs == -1 {
		s.nextKey = PastEndKey
	}

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

func cutBetween(in []byte, start []byte, end []byte) []byte {
	startIdx := bytes.Index(in, start)
	if startIdx > -1 {
		endIdx := bytes.Index(in, end)
		if endIdx >= startIdx {
			endIdx += len(end)
			bytesCut := endIdx - startIdx
			// unusual to move the front forward, instead of the end back, but we
			// know there's much less content before our target strings than after
			copy(in[bytesCut:], in[:startIdx])
			return in[bytesCut:]
		}
	}
	return in
}

var commentTag = []byte("      <comment>")
var commentCloseTag = []byte("</comment>\n")
var contributorTag = []byte("      <contributor>")
var contributorCloseTag = []byte("      </contributor>\n")
var minorTag = []byte("      <minor />\n")
var textStart = []byte("<text")

func cutMeta(in []byte) []byte {
	metaEnd := bytes.Index(in, textStart)
	if metaEnd == -1 {
		return in
	}
	meta := in[:metaEnd]
	meta = cutBetween(meta, commentTag, commentCloseTag)
	meta = cutBetween(meta, contributorTag, contributorCloseTag)
	meta = cutBetween(meta, minorTag, minorTag)
	bytesCut := metaEnd - len(meta)
	return in[bytesCut:]
}
