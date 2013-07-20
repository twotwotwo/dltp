// Public domain, Randall Farmer, 2013

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/twotwotwo/dltp/dpfile"
	"github.com/twotwotwo/dltp/zip"

	// for -cut mode
	chunk "github.com/twotwotwo/dltp/mwxmlchunk"
)

/* WRAPPERS */

const OutSuffix string = ".dltp"

func WriteDiffPack(out io.WriteCloser, inNames []string) {
	if len(inNames) < 2 {
		panic("need at least an input file and a source file")
	}
	// open outfile
	if out == nil {
		outName := inNames[0]
		outName = strings.Replace(outName, ".gz", "", 1)
		outName = strings.Replace(outName, ".bz2", "", 1)
		outName += OutSuffix

		compression := ""
		if !(*bz2 || *gz || *xz || *raw) {
			if zip.CanWrite("bz2") {
				*bz2 = true
			} else if zip.CanWrite("gz") {
				*gz = true
			} else {
				*raw = true
			}
		}
		if *bz2 {
			compression = "bz2"
		} else if *gz {
			compression = "gz"
		} else if *xz {
			compression = "xz"
		}

		if compression != "" {
			outName += "." + compression
		}
		outFile, err := os.Create(outName)
		if err != nil {
			panic(err)
		}

		if compression != "" {
			out = zip.NewWriter(outFile, compression)
		} else {
			out = outFile
		}
	}
	// newwriter
	w := dpfile.NewWriter(out, inNames, *lastRev, limitToNS, ns, *cutMeta)
	for w.WriteSegment() {
	}
	w.Close()
}

func ReadDiffPack(dp *os.File) {
	// use its dir as the working dir

	// defaults
	workingDirName := "."
	compression := ""
	if dp != os.Stdin {
		workingDirName = path.Dir(dp.Name())
		if strings.HasSuffix(dp.Name(), ".gz") {
			compression = "gz"
		}
		if strings.HasSuffix(dp.Name(), ".bz2") {
			compression = "bz2"
		}
		if strings.HasSuffix(dp.Name(), ".xz") {
			compression = "xz"
		}
		if strings.HasSuffix(dp.Name(), ".dp") {
			compression = ""
		}
	}

	workingDir, err := os.Open(workingDirName)
	if err != nil {
		panic(err)
	}

	input := io.Reader(nil)
	if compression != "" {
		input, err = zip.NewReader(dp, compression)
	} else {
		input = dp
	}
	if err != nil {
		panic(err)
	}

	// make the reader; log the filename
	streaming := dp == os.Stdin
	if *useFile {
		streaming = false
	}
	if *useStdout {
		streaming = true
	}
	r := dpfile.NewReader(input, workingDir, streaming)
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

/* COMMAND-LINE HANDLING */

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")
var useStdout = flag.Bool("c", false, "write to stdout even if unpacking file")
var useFile = flag.Bool("f", false, "write to file even if unpacking stdin")
var lastRev = flag.Bool("lastrev", false, "remove all but last rev in incr XML")
var nsString = flag.String("ns", "", "limit to pages in given <ns>")
var cutMeta = flag.Bool("cutmeta", false, "cut <contributor>/<comment>/<minor>")
var cut = flag.Bool("cut", false, "just output a cut down stdin (don't pack)")

var limitToNS = false
var ns = 0

var bz2 = flag.Bool("j", false, "try to use bzip")
var gz = flag.Bool("z", false, "try to use gzip")
var xz = flag.Bool("x", false, "try to use xz")
var raw = flag.Bool("r", false, "raw output")

func quitWith(format string, a ...interface{}) {
	fmt.Printf("Error: "+format+"\n", a...)
	os.Exit(255)
}

func main() {
	flag.Parse()
	args := flag.Args()

	if *cut {
		if *bz2 || *gz || *xz || *raw || *useStdout || *useFile {
			quitWith("only -lastrev, -ns, and -cutmeta work with -cut")
		}
		if !(*lastRev || *cutMeta || *nsString != "") {
			quitWith("use some of -lastrev, -ns, and -cutmeta with -cut")
		}
		if len(args) > 0 {
			quitWith("-cut only streams from stdin to stdout")
		}
	} else if len(args) < 2 { // validate other args as if unpacking
		if *bz2 || *gz || *xz || *raw {
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
		compressOpts := 0
		if *bz2 {
			if !zip.CanWrite("bz2") {
				quitWith("can't write .bz2 on this system")
			}
			compressOpts++
		}
		if *gz {
			compressOpts++
		}
		if *xz {
			if !zip.CanWrite("xz") {
				quitWith("can't write .xz on this system")
			}
			compressOpts++
		}
		if *raw {
			compressOpts++
		}
		if compressOpts > 1 {
			quitWith("you can only choose one compression option (-j/-z/-x/-r)")
		}
		if *useFile {
			quitWith("for now, -f is redundant when packing")
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
	} else if len(filenames) < 2 { //expand
		var dp *os.File
		var err error
		if len(filenames) == 1 {
			dp, err = os.Open(filenames[0])
			if err != nil {
				panic(err)
			}
		} else {
			dp = os.Stdin
		}
		ReadDiffPack(dp)
		os.Stdout.Close()
	} else { //pack
		WriteDiffPack(nil, filenames)
	}
}
