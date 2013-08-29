// Copyright 2011 The Go Authors; changes for dltp by Randall Farmer, 2013.
// All rights reserved.  Use of this source code is governed by a BSD-style
// license that can be found in Go's LICENSE file at http://golang.org/LICENSE

package bz2blocks

import (
	"bufio"
	"io"
)

// bitReader wraps an io.Reader and provides the ability to read values,
// bit-by-bit, from it. Its Read* methods don't return the usual error
// because the error handling was verbose. Instead, any error is kept and can
// be checked afterwards.
type bitReader struct {
	r    io.ByteReader
	n    uint64
	bits uint
	err  error
	eos  bool // hit end of bzip2 stream
	Pos  int64
}

// seekableByteReader is an io.ByteReader that uses ReadAt rather than Read
// to fill its buffer. (So it takes a ReaderAt, not a Seeker.) You can have
// many reading from different parts of a single ReaderAt at once.
type seekableByteReader struct {
	ra  io.ReaderAt
	pos int64
	buf []byte
	all [1 << 16]byte
	err error
}

func newByteReader(ra io.ReaderAt, bytePos int64) (br *seekableByteReader) {
	br = &seekableByteReader{ra: ra, pos: bytePos}
	br.fill()
	return
}

func (br *seekableByteReader) seek(pos int64) {
	br.pos = pos
	br.err = nil
	br.fill()
}

func (br *seekableByteReader) fill() {
	br.buf = br.all[:]
	n, err := br.ra.ReadAt(br.buf, br.pos)
	br.pos += int64(n)
	br.buf = br.buf[:n]
	br.err = err
}

func (br *seekableByteReader) ReadByte() (c byte, err error) {
	if len(br.buf) == 0 {
		br.fill()
		if len(br.buf) == 0 {
			return 0, br.err
		}
	}
	c = br.buf[0]
	br.buf = br.buf[1:]
	return
}

// newBitReader returns a new bitReader reading from r. If r is not
// already an io.ByteReader, it will be converted via a bufio.Reader.
func newBitReader(r io.Reader) bitReader {
	byter, ok := r.(io.ByteReader)
	if !ok {
		byter = bufio.NewReader(r)
	}
	return bitReader{r: byter}
}

// newBitReaderPos returns a new bitReader at a given bit position.
func newBitReaderPos(ra io.ReaderAt, pos int64) (br bitReader) {
	br = bitReader{r: newByteReader(ra, pos>>3)}
	br.ReadBits64(uint(pos & 7))
	br.Pos = pos
	return
}

// seeks to a bit position, if initialized with NewBitReaderPos; otherwise panics
func (br *bitReader) Seek(pos int64) {
	br.r.(*seekableByteReader).seek(pos >> 3)
	br.bits = 0
	br.n = 0
	br.err = nil
	br.ReadBits64(uint(pos & 7))
	br.Pos = pos
}

// ReadToBZBlock reads until it sees 0x314159265359 (block start) or
// 0x177245385090 (end of stream).
func (br *bitReader) ReadToBZBlock() (eos bool) {
	n := br.n & ((1 << br.bits) - 1)
	bits := br.bits
	n <<= 8
	br.Pos += int64(br.bits)
	for {
		br.Pos += 8
		bits += 8
		b, err := br.r.ReadByte()
		n |= uint64(b)
		if err == io.EOF {
			// OK because even the EOS marker must happen
			// before the last byte
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			br.err = err
			return true
		}
		if bits < 56 {
			n <<= 8
			continue
		}
		for i := uint(0); i < 8; i++ {
			masked := n & 0xFFFFFFFFFFFF00 // 56 bits
			if masked == 0x31415926535900 || masked == 0x17724538509000 {

				// "unread" whatever is in n, starting at the pattern
				n >>= i
				br.n = n
				br.bits = 56 - i
				// Pos reflected having read the pattern+remainder
				br.Pos -= int64(56 - i)

				return masked == 0x17724538509000
			}
			n <<= 1
		}
	}
}

// ReadBits64 reads the given number of bits and returns them in the
// least-significant part of a uint64. In the event of an error, it returns 0
// and the error can be obtained by calling Err().
func (br *bitReader) ReadBits64(bits uint) (n uint64) {
	br.Pos += int64(bits)
	for bits > br.bits {
		b, err := br.r.ReadByte()
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			br.err = err
			return 0
		}
		br.n <<= 8
		br.n |= uint64(b)
		br.bits += 8
	}

	// br.n looks like this (assuming that br.bits = 14 and bits = 6):
	// Bit: 111111
	//      5432109876543210
	//
	//         (6 bits, the desired output)
	//        |-----|
	//        V     V
	//      0101101101001110
	//        ^            ^
	//        |------------|
	//           br.bits (num valid bits)
	//
	// This the next line right shifts the desired bits into the
	// least-significant places and masks off anything above.
	n = (br.n >> (br.bits - bits)) & ((1 << bits) - 1)
	br.bits -= bits
	return
}

func (br *bitReader) ReadBits(bits uint) (n int) {
	n64 := br.ReadBits64(bits)
	return int(n64)
}

func (br *bitReader) ReadBit() bool {
	n := br.ReadBits(1)
	return n != 0
}

func (br *bitReader) Err() error {
	return br.err
}
