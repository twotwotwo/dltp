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
	"strings"

	"github.com/twotwotwo/dltp/dpfile"
	"github.com/twotwotwo/dltp/zip"
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
	w := dpfile.NewWriter(out, inNames)
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

/* COMMAND-LINE HANDLING */

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")
var useStdout = flag.Bool("c", false, "write to stdout even if unpacking file")
var useFile = flag.Bool("f", false, "write to file even if unpacking stdin")

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

	compressOpts := 0
	if *bz2 && !zip.CanWrite("bz2") {
		quitWith("can't write .bz2 on this system")
		compressOpts++
	}
	if *gz && !zip.CanWrite("gz") {
		quitWith("can't write .gz on this system")
		compressOpts++
	}
	if *xz && !zip.CanWrite("xz") {
		quitWith("can't write .xz on this system")
		compressOpts++
	}
	if *raw {
		compressOpts++
	}
	if compressOpts > 1 {
		quitWith("you can only choose one compression option (-j/-z/-x/-r)")
	}
	if (*bz2 || *gz || *xz || *raw) && len(args) < 2 {
		quitWith("compression options only work when packing")
	}
	// supporting - as source filename will obsolete these
	if *useFile && len(args) >= 2 {
		quitWith("for now, -f only necessary when unpacking from stdin")
	}
	if *useFile && *useStdout {
		quitWith("can't write both to stdin and to file")
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
	if len(filenames) < 2 { //expand
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
