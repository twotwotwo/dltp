// Public domain, Randall Farmer, 2013

package dpfile

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/twotwotwo/dltp/alloc"
	"github.com/twotwotwo/dltp/diff"
	"github.com/twotwotwo/dltp/mwxmlchunk"
	sref "github.com/twotwotwo/dltp/sourceref"
	"github.com/twotwotwo/dltp/stream"
	"github.com/twotwotwo/dltp/zip"
	"hash/crc64"
	"io"
	"os"
	"path"
	"regexp" // validating input filenames
	"runtime"
	"strings" // working w/filenames
)

/*

DIFFPACK FILE FORMAT

DiffPack files are gzipped, with a text preamble then binary data.

The text preamble has the following lines (each ending \n):

  - The format name (right now the literal string "DiffPack (test)")
  - the source URL (now a placeholder)
  - the format URL (now a placeholder)
  - a blank line
  - a list of files, starting with the output file
  - a blank line

Then they're followed by binary diffs each headed with a source reference, which
consists of three varints (written/read by SourceRef.Write and ReadSource):

  source file number (signed; -1 means no source)
  start offset (unsigned)
  source length (unsigned)

then the binary diff, which ends with a 0 instruction (see diff.Patch),
then the ECMA CRC-64 (the only fixed-size int in the format), then the
uncompressed length as an unsigned varint.

A source info header with ID, offset, and length all 0 marks the end of the
file.

The methods here are:

NewWriter(out, sources)         // write magic and filenames (incl input)
dpw.WriteSegment(source, text)  // write source number, start, end, diff, CRC
dpw.Close()                     // write end marker

NewReader(in, sources)          // open (including opening output)
dpr.ReadSegment()               // expand and write a page
dpr.Close()                     // flush outfile

The implementation does some gymnastics to package DiffTasks to run in the
background and have their output written later. It's not perfect, but seems to
help.

There are also vestiges of support for diffing successive revs of a page
against each other (e.g., in an incremental file). This would be nice to revive
but isn't that close now.

*/

// 386: individual values (segment lengths) need to be <2GB because of the ints
// here
func writeVarint(w io.Writer, val int) {
	var encBuf [10]byte
	i := binary.PutVarint(encBuf[:], int64(val))
	_, err := w.Write(encBuf[:i])
	if err != nil {
		panic("failed to write number (varint)")
	}
}

func writeUvarint(w io.Writer, val int) {
	var encBuf [10]byte
	i := binary.PutUvarint(encBuf[:], uint64(val))
	_, err := w.Write(encBuf[:i])
	if err != nil {
		panic("failed to write number (uvarint)")
	}
}

type DiffTask struct {
	s         diff.MatchState
	source    sref.SourceRef
	resultBuf []byte
	done      chan int
}

type DPWriter struct {
	out     *bufio.Writer
	zOut    io.WriteCloser
	sources []*mwxmlchunk.SegmentReader
	lastSeg []byte
	tasks   []DiffTask
	taskCh  chan *DiffTask
	slots   int
	winner  int
}

type DPReader struct {
	in      *bufio.Reader
	out     *bufio.Writer
	sources []io.ReaderAt
	lastSeg []byte
}

var MaxSourceLength = uint64(1e8)

var crcTable *crc64.Table

func NewWriter(zOut io.WriteCloser, sourceNames []string) (dpw DPWriter) {
	for i, name := range sourceNames {
		r, err := zip.Open(name)
		if err != nil {
			panic("cannot open source: " + err.Error())
		}
		f := stream.NewReaderAt(r)
		dpw.sources = append(dpw.sources, mwxmlchunk.NewSegmentReader(f, int64(i)))
	}
	dpw.zOut = zOut
	dpw.out = bufio.NewWriter(zOut)
	if crcTable == nil {
		crcTable = crc64.MakeTable(crc64.ECMA)
	}
	_, err := dpw.out.WriteString("DeltaPacker\nno format URL yet\nno source URL\n\n")
	if err != nil {
		panic(err)
	}
	for _, name := range sourceNames {
		niceOutName := path.Base(name)
		niceOutName = strings.Replace(niceOutName, ".gz", "", 1)
		niceOutName = strings.Replace(niceOutName, ".bz2", "", 1)
		fmt.Fprintln(dpw.out, niceOutName)
	}
	err = dpw.out.WriteByte('\n')
	if err != nil {
		panic(err)
	}
	dpw.out.Flush()

	runtime.GOMAXPROCS(runtime.NumCPU())
	dpw.slots = 100 // really a queue len, not thread count
	dpw.taskCh = make(chan *DiffTask, dpw.slots)
	for workerNum := 0; workerNum < runtime.NumCPU(); workerNum++ {
		go doDiffTasks(dpw.taskCh)
	}
	dpw.tasks = make([]DiffTask, dpw.slots)
	for i := range dpw.tasks {
		t := &dpw.tasks[i]
		t.s.Out = &bytes.Buffer{}
		t.done = make(chan int, 1)
		t.done <- 1
	}
	return
}

// a DiffTask wraps a MatchState with channel bookkeeping
func (t *DiffTask) Diff() { // really SegmentTask but arh
	bOrig := t.s.B // is truncated by Diff
	t.source.Write(t.s.Out)
	t.s.Diff()
	binary.Write(t.s.Out, binary.BigEndian, crc64.Checksum(bOrig, crcTable))
	writeUvarint(t.s.Out, len(bOrig))
	select {
	case t.done <- 1:
		return
	default:
		panic("same difftask being used twice!")
	}
}

func doDiffTasks(tc chan *DiffTask) {
	for t := range tc {
		t.Diff()
	}
}

func (dpw *DPWriter) WriteSegment() bool {
	// find the matching texts
	b := dpw.sources[0]
	a := dpw.sources[1:]
	source := sref.SourceNotFound
	aText := []byte(nil)
	bText, key, _, revFetchErr := b.ReadNext()
	if revFetchErr != nil && revFetchErr != io.EOF {
		panic(revFetchErr)
	}
	for _, src := range a {
		err := error(nil)
		aText, _, source, err = src.ReadTo(key)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if len(aText) > 0 {
			break
		}
	}
	// write something out
	if source.Length > MaxSourceLength {
		source = sref.SourceNotFound
		aText = nil
	}

	t := &dpw.tasks[dpw.winner%dpw.slots]
	<-t.done

	_, err := t.s.Out.WriteTo(dpw.out)
	if err != nil {
		panic("failed to write output: " + err.Error())
	}

	t.source = source
	t.s.A = alloc.CopyBytes(t.s.A, aText)
	t.s.B = alloc.CopyBytes(t.s.B, bText)
	t.s.Out.Reset()
	dpw.taskCh <- t
	dpw.winner++

	if revFetchErr == io.EOF {
		return false
	}
	return true
}

func (dpw *DPWriter) Close() {
	for i := range dpw.tasks { // heh, we have to use i
		t := &dpw.tasks[(dpw.winner+i)%dpw.slots]
		<-t.done
		t.s.Out.WriteTo(dpw.out)
	}
	close(dpw.taskCh)
	sref.EOFMarker.Write(dpw.out)
	dpw.out.Flush()
	if dpw.zOut != nil {
		dpw.zOut.Close()
	}
	//fmt.Println("Packed successfully")
}

func readLineOrPanic(in *bufio.Reader) string {
	line, err := in.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			panic("Premature EOF reading line")
		} else {
			panic(err)
		}
	}
	if len(line) > 0 {
		return line[:len(line)-1] // chop off \n
	}
	return line
}

var safeFilenamePat *regexp.Regexp

const safeFilenameStr = "^[a-zA-Z0-9_\\-.]*$"

func panicOnUnsafeName(filename string) string {
	if safeFilenamePat == nil {
		safeFilenamePat = regexp.MustCompile(safeFilenameStr)
	}
	if !safeFilenamePat.MatchString(filename) {
		panic(fmt.Sprint("unsafe filename: ", filename))
	}
	return filename
}

// these panics should probably be more informative quitWiths, or possibly
// error returns
func NewReader(in io.Reader, workingDir *os.File, streaming bool) (dpr DPReader) {
	dpr.in = bufio.NewReader(in)

	if crcTable == nil {
		crcTable = crc64.MakeTable(crc64.ECMA)
	}

	formatName := readLineOrPanic(dpr.in)
	expectedFormatName := "DeltaPacker"
	if formatName != expectedFormatName {
		fmt.Println("Expected format name:", expectedFormatName)
		panic("Doesn't look like the right file format")
	}

	formatUrl := readLineOrPanic(dpr.in)
	if formatUrl != "no format URL yet" {
		panic("Format URL doesn't look compatible with this version")
	}

	sourceUrl := readLineOrPanic(dpr.in) // discard source URL
	if sourceUrl == "" {
		panic("Expected a non-blank source URL line")
	}

	expectedBlank := readLineOrPanic(dpr.in)
	if expectedBlank != "" {
		panic("Expected a blank line after source URL")
	}

	// open the first source, a.k.a. the output, for writing:
	dirName := workingDir.Name()
	outputName := panicOnUnsafeName(readLineOrPanic(dpr.in))
	outputPath := path.Join(dirName, outputName)
	var outFile *os.File
	var err error
	if streaming {
		outFile = os.Stdout
	} else {
		outFile, err = os.Create(outputPath)
		if err != nil {
			panic("cannot create output")
		}
	}
	dpr.out = bufio.NewWriter(outFile)
	// open all sources for reading, including the output
	for sourceName := outputName; sourceName != ""; sourceName = panicOnUnsafeName(readLineOrPanic(dpr.in)) {
		if streaming && sourceName == outputName {
			dpr.sources = append(dpr.sources, nil) // don't read from me!
			continue
		}
		sourcePath := path.Join(dirName, sourceName)
		zipReader, err := zip.Open(sourcePath)
		if err != nil {
			panic("could not open source " + sourceName + ": " + err.Error())
		}
		stream := stream.NewReaderAt(zipReader)
		dpr.sources = append(dpr.sources, stream)
	}
	if len(dpr.sources) < 2 {
		panic("Need at least one source besides the output")
	}

	// we've read the blank line so we're ready for business
	return
}

var readBuf []byte // not parallel-safe, but reading isn't threaded

func (dpr *DPReader) ReadSegment() bool { // writes to self.out
	source := sref.ReadSource(dpr.in)
	if source == sref.EOFMarker {
		return false
	}
	if source.Length > MaxSourceLength {
		//fmt.Println("Max source len set to", MaxSourceLength)
		panic("input file (segment) using too large a source")
	}
	readBuf = alloc.Bytes(readBuf, int(source.Length))
	orig := readBuf
	// TODO: validate source number, start, length validity here
	if source == sref.PreviousSegment {
		panic("segment chaining not implemented")
	} else if source != sref.SourceNotFound {
		if int(source.SourceNumber) >= len(dpr.sources) {
			panic("too-high source number provided")
		}
		srcFile := dpr.sources[source.SourceNumber]
		_, err := srcFile.ReadAt(orig, int64(source.Start))
		if err != nil {
			//fmt.Println("error reading from source", source)
			panic(err)
		}
	}
	text := diff.Patch(orig, dpr.in)
	dpr.lastSeg = text
	_, err := dpr.out.Write(text)
	if err != nil {
		panic("couldn't write expanded file")
	}
	crc := crc64.Checksum(text, crcTable)
	var fileCrc uint64
	err = binary.Read(dpr.in, binary.BigEndian, &fileCrc)
	if err != nil {
		panic("couldn't read expected CRC")
	}
	if crc != fileCrc {
		panic("CRC mismatch")
	}
	length, err := binary.ReadUvarint(dpr.in)
	if err != nil {
		panic("couldn't read uncompressed len")
	}
	if int(length) != len(text) { // 386: segments limited to 2GB (OK)
		panic("incorrect uncompressed length")
	}
	return true
}

func (dpr *DPReader) Close() {
	dpr.out.Flush()
}
