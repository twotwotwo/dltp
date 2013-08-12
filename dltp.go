// Public domain, Randall Farmer, 2013

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/twotwotwo/dltp/dpfile"
	"github.com/twotwotwo/dltp/stream"
	"github.com/twotwotwo/dltp/zip"

	// for -cut mode
	chunk "github.com/twotwotwo/dltp/mwxmlchunk"
)

/* WRAPPERS */

const OutSuffix string = ".dltp"

func WriteDiffPack(out io.WriteCloser, workingDir *os.File, inNames []string) {
	if len(inNames) < 2 {
		panic("need at least an input file and a source file")
	}
	// open outfile
	if out == nil {
		// baseName is right for both URLs + Windows file paths
		baseName := path.Base(filepath.Base(inNames[0]))
		outName := zip.UnzippedName(baseName) + OutSuffix

		if *compression != "" {
			outName += "." + *compression
		}
		outFile, err := os.Create(path.Join(workingDir.Name(), outName))
		if err != nil {
			panic(err)
		}

		if *compression != "" {
			out = zip.NewWriter(outFile, *compression)
		} else {
			out = outFile
		}
	}
	// newwriter
	w := dpfile.NewWriter(out, workingDir, inNames, *lastRev, limitToNS, ns, *cutMeta)
	for w.WriteSegment() {
	}
	w.Close()
}

func ReadDiffPack(dp io.Reader, workingDir *os.File, streaming bool) {
	if *useFile {
		streaming = false
	}
	if *useStdout {
		streaming = true
	}
	r := dpfile.NewReader(dp, workingDir, streaming)
	// readsegment while we can
	for r.ReadSegment() {
	}
	// finish
	r.Close()
}

func CutStdinToStdout() {
	r := chunk.NewSegmentReader(os.Stdin, 0, *lastRev, limitToNS, ns, *cutMeta)
	for {
		text, _, _, err := r.ReadNext()
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}
		_, err = os.Stdout.Write(text)
		if err != nil {
			panic(err)
		}
	}
}

func Merge(in []io.Reader, out io.Writer) {
	readers := make([]*chunk.SegmentReader, len(in))
	for i, f := range in {
		readers[i] = chunk.NewSegmentReader(f, int64(i), *lastRev, limitToNS, ns, *cutMeta)
	}
	lastKey := chunk.BeforeStart
	keys := make([]chunk.SegmentKey, len(in))
	for i, _ := range keys {
		keys[i] = lastKey
	}
	text := make([][]byte, len(in))
	for lastKey != chunk.PastEndKey {
		// advance each past lastKey
		for i, r := range readers {
			for keys[i] <= lastKey {
				txt, key, _, err := r.ReadNext()
				text[i], keys[i] = txt, key
				if err != nil {
					if err != io.EOF {
						panic(err)
					}
				}
			}
		}
		// look for lowest value
		lastKey = chunk.PastEndKey
		for _, key := range keys {
			if key < lastKey {
				lastKey = key
			}
		}
		// print the text for leftmost instance of it
		for i, key := range keys {
			if key == lastKey {
				_, err := out.Write(text[i])
				if err != nil {
					panic(err)
				}
				break
			}
		}
	}
}

/* COMMAND-LINE HANDLING */

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")
var useStdout = flag.Bool("c", false, "write to stdout even if unpacking file")
var useFile = flag.Bool("f", false, "write to file even if unpacking stdin")
var lastRev = flag.Bool("lastrev", false, "remove all but last rev in incr XML")
var nsString = flag.String("ns", "", "limit to pages in given <ns>")
var cutMeta = flag.Bool("cutmeta", false, "cut <contributor>/<comment>/<minor>")
var cut = flag.Bool("cut", false, "just output a cut down stdin (don't pack)")
var merge = flag.Bool("merge", false, "merge files listed on command line (newest first) to stdout")
var debug = flag.Bool("debug", false, "on error, show ugly but useful debug info")
var compression = flag.String("zip", "auto", "set output compression (bz2, gz, lzo, none)")

var limitToNS = false
var ns = 0

func recoverAndPrintError() {
	if r := recover(); r != nil {
		fmt.Println("Error: ", r)
		os.Exit(255)
	}
}

func quitWith(format string, a ...interface{}) {
	fmt.Printf("Error: "+format+"\n", a...)
	os.Exit(255)
}

func main() {
	flag.Parse()
	args := flag.Args()

	if !*debug {
		defer recoverAndPrintError()
	}

	if *merge {
		if *useStdout || *useFile {
			quitWith("only -lastrev, -ns, and -cutmeta work with -merge")
		}
	} else if *cut {
		if *useStdout || *useFile {
			quitWith("only -lastrev, -ns, and -cutmeta work with -cut")
		}
		if *merge {
			quitWith("leave out -cut when using -merge")
		}
		if !(*lastRev || *cutMeta || *nsString != "") {
			quitWith("use some of -lastrev, -ns, and -cutmeta with -cut")
		}
		if len(args) > 0 {
			quitWith("-cut only streams from stdin to stdout")
		}
	} else if len(args) < 2 { // validate other args as if unpacking
		if *compression != "auto" {
			quitWith("compression options only work when packing")
		}
		if *useFile && *useStdout {
			quitWith("can't write both to stdin and to file")
		}
		if *lastRev {
			quitWith("-lastrev only used when packing")
		}
		if *nsString != "" {
			quitWith("-ns only used when packing")
		}
	} else { // validate as if packing
		if *compression == "auto" {
			if zip.CanWrite("bz2") {
				*compression = "bz2"
			} else {
				*compression = "gz"
			}
		}
		if *compression == "none" {
			*compression = ""
		}
		*compression = zip.CanonicalFormatName(*compression)
		if !zip.IsKnown(*compression) {
			quitWith("unknown compression type '" + *compression + "'")
		}
		if !zip.CanWrite(*compression) {
			quitWith("can't find (un)packer for ." + *compression)
		}
		if *useFile {
			quitWith("-f is redundant when packing")
		}
		if *useStdout {
			quitWith("-c not allowed when packing (won't pack to stdout)")
		}

		if *nsString != "" {
			limitToNS = true
			var err error
			ns, err = strconv.Atoi(*nsString)
			if err != nil {
				quitWith("ns must be an integer")
			}
		}
	}

	// with help from http://blog.golang.org/profiling-go-programs
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.WriteHeapProfile(f)
		f.Close()
		return
	}

	filenames := args[:]
	if *cut {
		CutStdinToStdout()
	} else if *merge {
		var sources = make([]io.Reader, len(filenames))
		for i, fn := range filenames {
			f, err := zip.Open(fn, nil)
			if err != nil {
				quitWith("can't open source " + fn + ": " + err.Error())
			}
			sources[i] = f
		}
		Merge(sources, os.Stdout)
	} else if len(filenames) < 2 { //expand
		var dp stream.Stream
		var err error

		// decide on working dir
		var workingDir *os.File
		if len(filenames) == 0 || strings.HasPrefix(filenames[0], "http://") {
			currentDir, err := os.Getwd()
			if err != nil {
				quitWith("can't get current dir, what kind of nonsense?")
			}
			workingDir, err = os.Open(currentDir)
		} else {
			currentDir := filepath.Dir(filenames[0])
			workingDir, err = os.Open(currentDir)
		}
		if err != nil {
			quitWith("can't open source directory")
		}

		streaming := false // we're always streaming, but sometimes stdin's involved
		if len(filenames) == 1 {
			dp, err = zip.Open(filenames[0], workingDir)
			if err != nil {
				quitWith("can't open source " + filenames[0] + ": " + err.Error())
			}
		} else {
			dp = os.Stdin
			streaming = true
		}
		ReadDiffPack(dp, workingDir, streaming)

		os.Stdout.Close()
	} else { //pack
		dir := filepath.Dir(filenames[0])
		if strings.HasPrefix(filenames[0], "http://") {
			dir = "."
		}
		dirFile, err := os.Open(dir)
		if err != nil {
			panic(err)
		}
		WriteDiffPack(nil, dirFile, filenames)
	}
}
