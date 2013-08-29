// Copyright 2011 The Go Authors; changes for dltp by Randall Farmer, 2013.
// All rights reserved.  Use of this source code is governed by a BSD-style
// license that can be found in Go's LICENSE file at http://golang.org/LICENSE

// Package bz2blocks implements bzip2 decompression, with (optional)
// parallel decompression and indexing to allow random access with ReadAt.
// It'd derived from the compress/bzip2 package in Go 1.1.
package bz2blocks

import (
	"encoding/binary" // to read/write block index
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"runtime"
	"sync"
)

// There's no RFC for bzip2. I used the Wikipedia page for reference and a lot
// of guessing: http://en.wikipedia.org/wiki/Bzip2
// The source code to pyflate was useful for debugging:
// http://www.paul.sladen.org/projects/pyflate

// A StructuralError is returned when the bzip2 data is found to be
// syntactically invalid.
type StructuralError string

func (s StructuralError) Error() string {
	return "bzip2 data invalid: " + string(s)
}

type parallelReader struct {
	ra         io.ReaderAt
	jobs       []job
	br         bitReader // for finding (assumed) block boundaries
	currBitPos int64     // where the next block should be
	blockNum   int
	n          int64 // bytes written
	blocks     blockList
	err        error
}

type job struct {
	*reader
	startBitPos int64
	err         error
	ready       chan bool
	blockNum    int
	isNewBlock  bool
}

// NewParallelReader reads from an bzip2 file (which must be a ReaderAt),
// trying to use as many threads as useful for decompression.
func NewParallelReader(ra io.ReaderAt) (r io.Reader) {
	pr := &parallelReader{
		br:         newBitReaderPos(ra, 32),
		currBitPos: 32,
	}
	pr.jobs = make([]job, runtime.GOMAXPROCS(0)+2)
	for i, _ := range pr.jobs {
		bz2 := new(reader)
		bz2.br = newBitReaderPos(ra, 0)
		j := &pr.jobs[i]
		*j = job{
			reader:      bz2,
			ready:       make(chan bool, 1),
			startBitPos: -1,
		}
		j.singleBlock = true
		err := j.setup()
		if err != nil {
			pr.err = err
			return pr
		}
		j.ready <- true
		j.setupDone = true
	}
	pr.currBitPos = 32
	return pr
}

type parallelReaderIndex struct {
	*parallelReader
	indexOut io.Writer
}

// Write an index of the blocks in a bzip2 file to indexOut, using multiple
// threads.  You can pass the index to future calls to NewReaderAt.
func ParallelIndex(ra io.ReaderAt, indexOut io.Writer) (err error) {
	return NewIndexingParallelReader(ra, indexOut).Close()
}

// NewIndexingParallelReader allows you to save an index the of blocks in a
// bzip2 file after reading its content, using multiple threads.  You may
// provide that index to NewReaderAt for random access to content later.
//
// To use, first read data from the ReadCloser returned, then Close it to
// write the block index to indexOut.  If you don't want to read the data,
// just use ParallelIndex instead.
func NewIndexingParallelReader(ra io.ReaderAt, indexOut io.Writer) (r io.ReadCloser) {
	return &parallelReaderIndex{
		parallelReader: NewParallelReader(ra).(*parallelReader),
		indexOut:       indexOut,
	}
}

// Writes index to the indexOut passed to NewIndexingParallelReader. If the
// underlying ReaderAt is a ReadCloser, also closes it.
func (pri *parallelReaderIndex) Close() (err error) {
	if pri.err == nil { // we're not done yet!
		io.Copy(ioutil.Discard, pri)
	}
	if pri.err != io.EOF {
		return err
	}
	pri.jobs[0].blocks = pri.blocks
	err = pri.jobs[0].writeIndex(pri.indexOut)
	if err != nil {
		return err
	}
	if cl, ok := pri.ra.(io.Closer); ok {
		err = cl.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (j *job) run() {
	j.err = nil
	j.br.Seek(j.startBitPos)
	magic := j.br.ReadBits64(48)
	if magic != bzip2BlockMagic {
		j.err = errors.New(fmt.Sprintf("incorrect block magic on block %d: %X", j.blockNum, magic))
		j.ready <- true
	}
	j.err = j.readBlock()
	if j.br.Err() != nil {
		j.err = j.br.Err()
	}
	j.isNewBlock = true
	j.ready <- true
}

func (pr *parallelReader) Read(p []byte) (n int, err error) {
	if pr.err != nil {
		return 0, pr.err
	}
	for {

		j := &pr.jobs[pr.blockNum%len(pr.jobs)]
		<-j.ready

		if j.startBitPos == pr.currBitPos {

			if j.isNewBlock == true {
				pr.blocks = append(pr.blocks, blockBoundary{InBitPos: pr.currBitPos, OutBytePos: pr.n})
				j.isNewBlock = false
			}

			bytes, readErr := j.Read(p)
			n += bytes
			pr.n += int64(bytes)
			p = p[bytes:]

			if readErr != nil {
				j.err = readErr
			}
			if j.err != nil {
				pr.err = j.err
				return n, j.err
			}
			if len(p) == 0 {
				// keep it ready for next time
				j.ready <- true
				return
			}

			pr.currBitPos = j.br.Pos
		}

		if isEOF := pr.br.ReadToBZBlock(); !isEOF {
			j.startBitPos = pr.br.Pos
			j.blockNum = pr.blockNum
			go j.run()
		} else {
			j.eof = true
			j.startBitPos = pr.br.Pos
			j.ready <- true
		}

		pr.blockNum++
	}
}

// A reader decompresses bzip2 compressed data.
type readerBase struct {
	br        bitReader
	setupDone bool // true if we have parsed the bzip2 header.
	blockSize int  // blockSize in bytes, i.e. 900 * 1024.
	eof       bool
	buf       []byte    // stores Burrows-Wheeler transformed data.
	c         [256]uint // the `C' array for the inverse BWT.
	tt        []uint32  // mirrors the `tt' array in the bzip2 source and contains the P array in the upper 24 bits.
	tPos      uint32    // Index of the next output byte in tt.

	preRLE      []uint32 // contains the RLE data still to be processed.
	preRLEUsed  int      // number of entries of preRLE used.
	lastByte    int      // the last byte value seen.
	byteRepeats uint     // the number of repeats of lastByte seen.
	repeats     uint     // the number of copies of lastByte to output.

	n           int64     // bytes read
	blocks      blockList // for indexing or ReadAt
	indexOut    io.Writer // for indexing
	cl          io.Closer // for Close()
	singleBlock bool      // for parallelReader
	setupErr    error     // for NewReaderAt
}

type reader struct {
	readerBase
}

type readerAt struct {
	ra io.ReaderAt
	m  sync.Mutex // ReaderAt required to be safe for parallel reads
	readerBase
}

// NewReader returns a Reader which decompresses bzip2 data from r.
func NewReader(r io.Reader) io.Reader {
	bz2 := new(reader)
	bz2.br = newBitReader(r)
	return bz2
}

// Write an index of the blocks in a bzip2 file to indexOut.  You can pass
// the index to future calls to NewReaderAt.
func Index(r io.Reader, indexOut io.Writer) (err error) {
	return NewIndexingReader(r, indexOut).Close()
}

// Allows you to save an index the of blocks in a bzip2 file after reading
// its content.  You may provide that index to NewReaderAt for random access
// to content later.
//
// First, read data from the ReadCloser returned, then Close it to write the
// block index to indexOut.  If you don't want to read the data, just use
// ParallelIndex.
func NewIndexingReader(r io.Reader, indexOut io.Writer) io.ReadCloser {
	bz2 := new(reader)
	bz2.br = newBitReader(r)
	bz2.indexOut = indexOut
	return bz2
}

// Makes a ReaderAt given a bzip file and an index created by
// NewIndexingReader or NewIndexingParallelReader.  Note that each call to
// ReadAt decompresses at least a bzip2 block (100-900KB) and decompressed
// data is not reused across calls.
func NewReaderAt(ra io.ReaderAt, indexIn io.Reader) io.ReaderAt {
	bz2 := new(readerAt)
	bz2.ra = ra
	bz2.br = newBitReaderPos(ra, 0)
	err := bz2.readIndex(indexIn)
	if err != nil {
		bz2.setupErr = err
	}
	if bz2.br.Err() != nil {
		bz2.setupErr = bz2.br.Err()
	}
	return bz2
}

const bzip2FileMagic = 0x425a // "BZ"
const bzip2BlockMagic = 0x314159265359
const bzip2FinalMagic = 0x177245385090

// setup parses the bzip2 header.
func (bz2 *readerBase) setup() error {
	br := &bz2.br

	magic := br.ReadBits(16)
	if magic != bzip2FileMagic {
		return StructuralError("bad magic value")
	}

	t := br.ReadBits(8)
	if t != 'h' {
		return StructuralError("non-Huffman entropy encoding")
	}

	level := br.ReadBits(8)
	if level < '1' || level > '9' {
		return StructuralError("invalid compression level")
	}

	bz2.blockSize = 100 * 1024 * (int(level) - '0')
	bz2.tt = make([]uint32, bz2.blockSize)
	return nil
}

func (bz2 *reader) Read(buf []byte) (n int, err error) {
	if bz2.eof {
		return 0, io.EOF
	}
	if bz2.setupErr != nil {
		return 0, bz2.setupErr
	}

	if !bz2.setupDone {
		err = bz2.setup()
		brErr := bz2.br.Err()
		if brErr != nil {
			err = brErr
		}
		if err != nil {
			return 0, err
		}
		bz2.setupDone = true
	}

	n, err = bz2.read(buf)
	brErr := bz2.br.Err()
	if brErr != nil {
		err = brErr
	}
	return
}

// Writes index to the indexOut passed to NewIndexingReader, if any. If the
// underlying Reader is a ReadCloser, also closes it.
func (bz2 *reader) Close() (err error) {
	if bz2.setupErr != nil {
		return bz2.setupErr
	}
	if bz2.cl != nil {
		err = bz2.cl.Close()
		if err != nil {
			return err
		}
	}
	if bz2.indexOut != nil {
		if !bz2.eof {
			_, err = io.Copy(ioutil.Discard, bz2)
			if err != nil {
				return err
			}
		}
		err = bz2.writeIndex(bz2.indexOut)
		if err != nil {
			return err
		}
	}
	return
}

func (bz2 *readerAt) ReadAt(buf []byte, off int64) (n int, err error) {
	bz2.m.Lock()
	defer bz2.m.Unlock()
	startBitPos := int64(-1)
	startBytePos := int64(-1)
	for _, b := range bz2.blocks {
		if b.OutBytePos <= off {
			startBitPos = b.InBitPos
		}
	}
	if startBitPos == -1 {
		return 0, io.EOF
	}

	bz2.br = newBitReaderPos(bz2.ra, startBitPos)

	// discard the beginning of the block
	remaining := off - startBytePos
	var discardBuf [1 << 12]byte
	for remaining > 0 {
		toRead := remaining
		if toRead > 1<<12 {
			toRead = 1 << 12
		}
		n, err = bz2.read(discardBuf[:toRead])
		remaining -= int64(n)
		brErr := bz2.br.Err()
		if brErr != nil {
			err = brErr
		}
		if err != nil {
			return
		}
	}

	// TODO: actually have to read as many times as it takes to fill the block
	n, err = bz2.read(buf)
	brErr := bz2.br.Err()
	if brErr != nil {
		err = brErr
	}
	return
}

func (bz2 *readerBase) read(buf []byte) (n int, err error) {
	// bzip2 is a block based compressor, except that it has a run-length
	// preprocessing step. The block based nature means that we can
	// preallocate fixed-size buffers and reuse them. However, the RLE
	// preprocessing would require allocating huge buffers to store the
	// maximum expansion. Thus we process blocks all at once, except for
	// the RLE which we decompress as required.

	for (bz2.repeats > 0 || bz2.preRLEUsed < len(bz2.preRLE)) && n < len(buf) {
		// We have RLE data pending.

		// The run-length encoding works like this:
		// Any sequence of four equal bytes is followed by a length
		// byte which contains the number of repeats of that byte to
		// include. (The number of repeats can be zero.) Because we are
		// decompressing on-demand our state is kept in the Reader
		// object.

		if bz2.repeats > 0 {
			buf[n] = byte(bz2.lastByte)
			n++
			bz2.repeats--
			if bz2.repeats == 0 {
				bz2.lastByte = -1
			}
			continue
		}

		bz2.tPos = bz2.preRLE[bz2.tPos]
		b := byte(bz2.tPos)
		bz2.tPos >>= 8
		bz2.preRLEUsed++

		if bz2.byteRepeats == 3 {
			bz2.repeats = uint(b)
			bz2.byteRepeats = 0
			continue
		}

		if bz2.lastByte == int(b) {
			bz2.byteRepeats++
		} else {
			bz2.byteRepeats = 0
		}
		bz2.lastByte = int(b)

		buf[n] = b
		n++
	}

	if n > 0 || bz2.singleBlock {
		bz2.n += int64(n)
		return
	}

	// No RLE data is pending so we need to read a block.
	br := &bz2.br
	if bz2.indexOut != nil {
		bz2.blocks = append(bz2.blocks, blockBoundary{InBitPos: br.Pos, OutBytePos: bz2.n})
	}
	magic := br.ReadBits64(48)
	if magic == bzip2FinalMagic {
		br.ReadBits64(32) // ignored CRC
		bz2.eof = true
		return 0, io.EOF
	} else if magic != bzip2BlockMagic {
		return 0, StructuralError(fmt.Sprintf("bad magic value found: %X", magic))
	}

	err = bz2.readBlock()
	if err != nil {
		return 0, err
	}

	return bz2.read(buf)
}

// readBlock reads a bzip2 block. The magic number should already have been consumed.
func (bz2 *readerBase) readBlock() (err error) {
	br := &bz2.br
	br.ReadBits64(32) // skip checksum. TODO: check it if we can figure out what it is.
	randomized := br.ReadBits(1)
	if randomized != 0 {
		return StructuralError("deprecated randomized files")
	}
	origPtr := uint(br.ReadBits(24))

	// If not every byte value is used in the block (i.e., it's text) then
	// the symbol set is reduced. The symbols used are stored as a
	// two-level, 16x16 bitmap.
	symbolRangeUsedBitmap := br.ReadBits(16)
	symbolPresent := make([]bool, 256)
	numSymbols := 0
	for symRange := uint(0); symRange < 16; symRange++ {
		if symbolRangeUsedBitmap&(1<<(15-symRange)) != 0 {
			bits := br.ReadBits(16)
			for symbol := uint(0); symbol < 16; symbol++ {
				if bits&(1<<(15-symbol)) != 0 {
					symbolPresent[16*symRange+symbol] = true
					numSymbols++
				}
			}
		}
	}

	// A block uses between two and six different Huffman trees.
	numHuffmanTrees := br.ReadBits(3)
	if numHuffmanTrees < 2 || numHuffmanTrees > 6 {
		return StructuralError("invalid number of Huffman trees")
	}

	// The Huffman tree can switch every 50 symbols so there's a list of
	// tree indexes telling us which tree to use for each 50 symbol block.
	numSelectors := br.ReadBits(15)
	treeIndexes := make([]uint8, numSelectors)

	// The tree indexes are move-to-front transformed and stored as unary
	// numbers.
	mtfTreeDecoder := newMTFDecoderWithRange(numHuffmanTrees)
	for i := range treeIndexes {
		c := 0
		for {
			inc := br.ReadBits(1)
			if inc == 0 {
				break
			}
			c++
		}
		if c >= numHuffmanTrees {
			return StructuralError("tree index too large")
		}
		treeIndexes[i] = uint8(mtfTreeDecoder.Decode(c))
	}

	// The list of symbols for the move-to-front transform is taken from
	// the previously decoded symbol bitmap.
	symbols := make([]byte, numSymbols)
	nextSymbol := 0
	for i := 0; i < 256; i++ {
		if symbolPresent[i] {
			symbols[nextSymbol] = byte(i)
			nextSymbol++
		}
	}
	mtf := newMTFDecoder(symbols)

	numSymbols += 2 // to account for RUNA and RUNB symbols
	huffmanTrees := make([]huffmanTree, numHuffmanTrees)

	// Now we decode the arrays of code-lengths for each tree.
	lengths := make([]uint8, numSymbols)
	for i := 0; i < numHuffmanTrees; i++ {
		// The code lengths are delta encoded from a 5-bit base value.
		length := br.ReadBits(5)
		for j := 0; j < numSymbols; j++ {
			for {
				if !br.ReadBit() {
					break
				}
				if br.ReadBit() {
					length--
				} else {
					length++
				}
			}
			if length < 0 || length > 20 {
				return StructuralError("Huffman length out of range")
			}
			lengths[j] = uint8(length)
		}
		huffmanTrees[i], err = newHuffmanTree(lengths)
		if err != nil {
			return err
		}
	}

	selectorIndex := 1 // the next tree index to use
	currentHuffmanTree := huffmanTrees[treeIndexes[0]]
	bufIndex := 0 // indexes bz2.buf, the output buffer.
	// The output of the move-to-front transform is run-length encoded and
	// we merge the decoding into the Huffman parsing loop. These two
	// variables accumulate the repeat count. See the Wikipedia page for
	// details.
	repeat := 0
	repeat_power := 0

	// The `C' array (used by the inverse BWT) needs to be zero initialized.
	for i := range bz2.c {
		bz2.c[i] = 0
	}

	decoded := 0 // counts the number of symbols decoded by the current tree.
	for {
		if decoded == 50 {
			currentHuffmanTree = huffmanTrees[treeIndexes[selectorIndex]]
			selectorIndex++
			decoded = 0
		}

		v := currentHuffmanTree.Decode(br)
		decoded++

		if v < 2 {
			// This is either the RUNA or RUNB symbol.
			if repeat == 0 {
				repeat_power = 1
			}
			repeat += repeat_power << v
			repeat_power <<= 1

			// This limit of 2 million comes from the bzip2 source
			// code. It prevents repeat from overflowing.
			if repeat > 2*1024*1024 {
				return StructuralError("repeat count too large")
			}
			continue
		}

		if repeat > 0 {
			// We have decoded a complete run-length so we need to
			// replicate the last output symbol.
			for i := 0; i < repeat; i++ {
				b := byte(mtf.First())
				bz2.tt[bufIndex] = uint32(b)
				bz2.c[b]++
				bufIndex++
			}
			repeat = 0
		}

		if int(v) == numSymbols-1 {
			// This is the EOF symbol. Because it's always at the
			// end of the move-to-front list, and never gets moved
			// to the front, it has this unique value.
			break
		}

		// Since two metasymbols (RUNA and RUNB) have values 0 and 1,
		// one would expect |v-2| to be passed to the MTF decoder.
		// However, the front of the MTF list is never referenced as 0,
		// it's always referenced with a run-length of 1. Thus 0
		// doesn't need to be encoded and we have |v-1| in the next
		// line.
		b := byte(mtf.Decode(int(v - 1)))
		bz2.tt[bufIndex] = uint32(b)
		bz2.c[b]++
		bufIndex++
	}

	if origPtr >= uint(bufIndex) {
		return StructuralError("origPtr out of bounds")
	}

	// We have completed the entropy decoding. Now we can perform the
	// inverse BWT and setup the RLE buffer.
	bz2.preRLE = bz2.tt[:bufIndex]
	bz2.preRLEUsed = 0
	bz2.tPos = inverseBWT(bz2.preRLE, origPtr, bz2.c[:])
	bz2.lastByte = -1
	bz2.byteRepeats = 0
	bz2.repeats = 0

	return nil
}

// inverseBWT implements the inverse Burrows-Wheeler transform as described in
// http://www.hpl.hp.com/techreports/Compaq-DEC/SRC-RR-124.pdf, section 4.2.
// In that document, origPtr is called `I' and c is the `C' array after the
// first pass over the data. It's an argument here because we merge the first
// pass with the Huffman decoding.
//
// This also implements the `single array' method from the bzip2 source code
// which leaves the output, still shuffled, in the bottom 8 bits of tt with the
// index of the next byte in the top 24-bits. The index of the first byte is
// returned.
func inverseBWT(tt []uint32, origPtr uint, c []uint) uint32 {
	sum := uint(0)
	for i := 0; i < 256; i++ {
		sum += c[i]
		c[i] = sum - c[i]
	}

	for i := range tt {
		b := tt[i] & 0xff
		tt[c[b]] |= uint32(i) << 8
		c[b]++
	}

	return tt[origPtr] >> 8
}

type blockBoundary struct {
	InBitPos   int64
	OutBytePos int64
}

type blockList []blockBoundary

func (bz2 *reader) writeIndex(w io.Writer) (err error) {
	// writing the block count
	err = binary.Write(w, binary.BigEndian, uint64(len(bz2.blocks)))
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, uint64(bz2.blockSize))
	if err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, bz2.blocks)
}

func (bz2 *readerBase) readIndex(r io.Reader) (err error) {
	count := uint64(0)
	err = binary.Read(r, binary.BigEndian, &count)
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	if err != nil {
		return err
	}
	blockSize := uint64(0)
	err = binary.Read(r, binary.BigEndian, &blockSize)
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	if err != nil {
		return err
	}
	bz2.blockSize = int(blockSize)
	bz2.tt = make([]uint32, bz2.blockSize)
	bz2.blocks = make(blockList, count)
	err = binary.Read(r, binary.BigEndian, bz2.blocks)
	if err != nil && err != io.EOF {
		return err
	}
	bz2.setupDone = true
	return
}
