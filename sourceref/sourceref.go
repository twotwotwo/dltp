// Public domain, Randall Farmer, 2013

package sourceref

import (
    "io"
    "encoding/binary"
)

/*

Types for exchanging info about source references, for use by 
github.com/twotwotwo/dltp/mwxmlchunk and diffpack/dpfile.

It's not super clear this *couldn't* be part of github.com/twotwotwo/dltp/mwxmlchunk instead. But
*reading* requires a SourceRef but not the SegmentReader, so it seemed like you
should be able to use it without pulling the input-chunking logic in.

*/

type SourceRef struct {
	SourceNumber int64
	Start        uint64
	Length       uint64
}

var SourceNotFound = SourceRef{-1, 0, 0}
var PreviousSegment = SourceRef{-2, 0, 0}
var EOFMarker = SourceRef{0, 0, 0}

func (s SourceRef) Write(w io.Writer) {
  var encodingBuf [32]byte
	encodedSource := encodingBuf[:]
	// negative source numbers are special values
	i := binary.PutVarint(encodedSource, s.SourceNumber)
	i += binary.PutUvarint(encodedSource[i:], s.Start)
	i += binary.PutUvarint(encodedSource[i:], s.Length)
	_, err := w.Write(encodingBuf[:i])
	if err != nil {
	  //fmt.Fprintln(os.Stderr, err)
		panic("couldn't write source information")
	}
}

func ReadSource(r io.ByteReader) SourceRef {
	sourceNumber, err := binary.ReadVarint(r)
	if err != nil {
		panic("couldn't read source number")
	}
	start, err := binary.ReadUvarint(r)
	if err != nil {
		panic("couldn't read source offset")
	}
	length, err := binary.ReadUvarint(r)
	if err != nil {
		panic("couldn't read source length")
	}
	return SourceRef{int64(sourceNumber), uint64(start), uint64(length)}
}


