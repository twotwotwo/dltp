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
	"hash/fnv"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp" // validating input filenames
	"runtime"
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
then the 32-bit FNV-1a (the only fixed-size int in the format), then the
uncompressed length as an unsigned varint.

A source info header with ID, offset, and length all 0 marks the end of the
file.

The methods here are:

NewWriter(out, sources)         // write magic and filenames (incl input)
dpw.WriteSegment()              // write source number, start, end, diff, cksum
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

type checksum uint32

func dpchecksum(text []byte) checksum {
	h := fnv.New32a()
	h.Write(text)
	return checksum(h.Sum32())
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
	in         *bufio.Reader
	out        *bufio.Writer
	sources    []io.ReaderAt
	lastSeg    []byte
	ChangeDump bool
}

var MaxSourceLength = uint64(1e8)

func NewWriter(zOut io.WriteCloser, workingDir *os.File, sourceNames []string, lastRevOnly bool, limitToNS bool, ns int, cutMeta bool) (dpw DPWriter) {
	for i, name := range sourceNames {
		r, err := zip.Open(name, workingDir)
		if err != nil {
			panic("cannot open source: " + err.Error())
		}
		f := stream.NewReaderAt(r)
		dpw.sources = append(
			dpw.sources,
			mwxmlchunk.NewSegmentReader(f, int64(i), lastRevOnly, limitToNS, ns, cutMeta),
		)
		// only use snipping options when reading first source
		lastRevOnly = false
		limitToNS = false
		cutMeta = false
	}
	dpw.zOut = zOut
	dpw.out = bufio.NewWriter(zOut)
	_, err := dpw.out.WriteString("DeltaPacker\nno format URL yet\nno source URL\n\n")
	if err != nil {
		panic(err)
	}
	for _, name := range sourceNames {
		// baseName is right for both URLs + Windows file paths
		baseName := path.Base(filepath.Base(name))
		niceOutName := zip.UnzippedName(baseName)
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
	binary.Write(t.s.Out, binary.BigEndian, dpchecksum(t.s.A))
	t.s.Diff()
	binary.Write(t.s.Out, binary.BigEndian, dpchecksum(bOrig))
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
	t.s.A = append(t.s.A[:0], aText...)
	t.s.B = append(t.s.B[:0], bText...)
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

const safeFilenameStr = "^[-a-zA-Z0-9_.]*$"

func panicOnUnsafeName(filename string) string {
	if safeFilenamePat == nil {
		safeFilenamePat = regexp.MustCompile(safeFilenameStr)
	}
	if !safeFilenamePat.MatchString(filename) {
		panic(fmt.Sprint("unsafe filename: ", filename))
	}
	return filename
}

func NewReader(in io.Reader, workingDir *os.File, streaming bool) (dpr DPReader) {
	dpr.in = bufio.NewReader(in)

	formatName := readLineOrPanic(dpr.in)
	expectedFormatName := "DeltaPacker"
	badFormat := false
	if formatName != expectedFormatName {
		badFormat = true
	}

	formatUrl := readLineOrPanic(dpr.in)
	if formatUrl != "no format URL yet" {
		if formatUrl[:4] == "http" {
			panic("Format has been updated. Go to " + formatUrl + " for an updated version of this utility.")
		}
		badFormat = true
	}

	if badFormat {
		panic("Didn't see the expected format name in the header. Either the input isn't actually a dltp file or the format has changed you need to download a newer version of this tool.")
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
		zipReader, err := zip.Open(sourcePath, workingDir)
		if err != nil {
			panic("could not open source " + sourceName + ": " + err.Error())
		}
		dpr.sources = append(dpr.sources, zipReader)
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
		if dpr.ChangeDump {
			_, err := dpr.out.Write(dpr.lastSeg)
			if err != nil {
				panic("couldn't write expanded file")
			}
		}
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

	var sourceCksum checksum
	err := binary.Read(dpr.in, binary.BigEndian, &sourceCksum)
	if err != nil {
		panic("couldn't read expected checksum")
	}

	text := diff.Patch(orig, dpr.in)

	cksum := dpchecksum(text)
	var fileCksum checksum
	err = binary.Read(dpr.in, binary.BigEndian, &fileCksum)
	if err != nil {
		panic("couldn't read expected checksum")
	}

	if cksum != fileCksum {

		origCksum := dpchecksum(orig)
		panicMsg := ""
		if origCksum == sourceCksum {
			if sourceCksum == 0 { // no source checksum
				panicMsg = "checksum mismatch. it's possible you don't have the original file this diff was created against, or it could be a bug in dltp."
			} else {
				panicMsg = "sorry; it looks like source file you have isn't original file this diff was created against."
			}
		} else {
			panicMsg = "checksum mismatch. this looks likely to be a bug in dltp."
		}

		os.Remove("dltp-error-report.txt")
		crashReport, err := os.Create("dltp-error-report.txt")
		if err == nil {
			fmt.Fprintln(crashReport, panicMsg)
			fmt.Fprintln(crashReport, "SourceRef:", source)
			crashReport.WriteString("Original text:\n\n")
			crashReport.Write(orig)
			crashReport.WriteString("\n\nPatched output:\n\n")
			crashReport.Write(text)
			crashReport.Close()
			panicMsg += " wrote additional information to dltp-error-report.txt"
		} else {
			panicMsg += " couldn't write additional information (" + err.Error() + ")"
		}

		panic(panicMsg)
	}

	// write if not ChangeDump or if changed or if this is preamble
	if !dpr.ChangeDump || !bytes.Equal(text, orig) || dpr.lastSeg == nil {
		_, err := dpr.out.Write(text)
		if err != nil {
			panic("couldn't write expanded file")
		}
	}

	dpr.lastSeg = text

	return true
}

func (dpr *DPReader) Close() {
	dpr.out.Flush()
}
