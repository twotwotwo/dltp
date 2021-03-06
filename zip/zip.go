// Public domain, Randall Farmer, 2013

package zip

import (
	"compress/gzip" // fallback f/no pipeable gzip present (e.g., Windows)
	bzip2 "github.com/twotwotwo/dltp/bz2blocks"
	"github.com/twotwotwo/dltp/httpfile" // who doznt like it.
	"github.com/twotwotwo/dltp/stream"   // allow skipping fwd through streams
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings" // filename fun
)

// know what would be cool to support here? snappy
// https://code.google.com/p/snappy-go/
// same purpose lzo serves now ("free" compression to speed disk I/O)

/*

(UN)ZIP HELPER

Open(path string):
given path "a.xml", open a.xml, a.xml.gz, or a.xml.bz2, and either pipe via
a native decompressor or use go's own. either way, return a Reader.

NewWriter/NewReader:
pipe through a native compressor or use go's own gzip

*/

var suffixes = []string{"", ".lzo", ".gz", ".bz2", ".xz"}
var programs = map[string]string{
	"lzo": "lzop",
	"gz":  "pigz gzip",
	"bz2": "lbzip2 bzip2",
	"xz":  "xz",
}
var canonicalFormatNames = map[string]string{
	"bzip2": "bz2",
	"gzip":  "gz",
}

// Name without any known zip suffixes attached.
func UnzippedName(path string) string {
	previousPath := ""
	for previousPath != path {
		previousPath = path
		for _, suffix := range suffixes[1:] {
			if strings.HasSuffix(path, suffix) {
				path = path[:len(path)-len(suffix)]
			}
		}
	}
	return path
}

func CanonicalFormatName(compression string) string {
	if canonicalFormatNames[compression] != "" {
		return canonicalFormatNames[compression]
	}
	return compression
}

func IsKnown(compression string) bool {
	return programs[compression] != ""
}

func Open(path string, workingDir *os.File) (s stream.Stream, err error) {
	reader := stream.Stream(nil)
	fn := path

	if strings.HasPrefix(path, "http://") {
		reader, err = httpfile.Open(path, workingDir)
	} else {
		// try to open a raw file, then known compressed formats
		for _, suffix := range suffixes {
			fn = path + suffix
			reader, err = os.Open(fn)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				break
			}
			break
		}
	}

	// didn't find it, sigh
	if err != nil {
		return nil, err
	}

	var compressedReader io.Reader

	for _, suffix := range suffixes {
		if suffix == "" {
			continue
		}
		if !strings.HasSuffix(fn, suffix) {
			continue
		}
		compressedReader, err = NewReader(reader, suffix[1:])
		if err != nil {
			return nil, err
		}
		break
	}

	// return a Reader/ReaderAt, either file or wrapper
	if compressedReader == nil {
		return reader, nil
	} else {
		return stream.NewReaderAt(compressedReader), nil
	}
}

type CmdPipe struct {
	cmd    *exec.Cmd
	writer io.WriteCloser
}

func (c *CmdPipe) Write(p []byte) (n int, err error) {
	return c.writer.Write(p)
}

func (c *CmdPipe) Close() error {
	err := c.writer.Close()
	if err != nil {
		c.cmd.Wait()
		return err
	}
	return c.cmd.Wait()
}

func findZipper(format string) (cmdPath string) {
	if format == "" {
		return
	}

	choicesStr := programs[format]
	if choicesStr == "" {
		panic("unknown compression format " + format)
	}

	choices := strings.Split(choicesStr, " ")
	for _, cmd := range choices {
		cmdPath, _ = exec.LookPath(cmd)
		if cmdPath != "" {
			return
		}
	}

	return
}

func CanWrite(format string) bool {
	if format == "gz" {
		return true
	}
	return findZipper(format) != ""
}

func NewWriter(out io.Writer, format string) io.WriteCloser {
	cmdPath := findZipper(format)
	if cmdPath == "" {
		if format == "gz" {
			return gzip.NewWriter(out)
		} else {
			panic("cannot write format " + format)
		}
	}
	cmd := exec.Command(cmdPath, "-c")
	cmd.Stdout = out
	cmd.Stderr = os.Stderr
	writer, err := cmd.StdinPipe()
	if err != nil {
		panic(err)
	}
	cmd.Start()
	// not clear to me that writer.Close() would or could wait on zipper to
	// exit. so wrap it to add waiting
	return &CmdPipe{cmd, writer}
}

type UnsupportedFormat struct {
	format string
}

func (e UnsupportedFormat) Error() string {
	return "unsupported format " + e.format
}

// act as an io.Reader but make Close() copy the rest of the content to a blackhole,
// to avoid a "broken pipe" error from the process we're streaming from (we don't want
// to hide all errors from the user, but this one's useless)
type FinishingReader struct {
	io.Reader
}

func NewFinishingReader(in io.Reader) (fr *FinishingReader) {
	return &FinishingReader{in}
}

func (fr *FinishingReader) Close() error {
	_, err := io.Copy(ioutil.Discard, fr.Reader)
	return err
}

func NewReader(in io.Reader, format string) (rc io.Reader, err error) {
	cmdPath := findZipper(format)
	if cmdPath == "" {
		if format == "gz" {
			return gzip.NewReader(in)
		} else if format == "bz2" {
			if ra, ok := in.(io.ReaderAt); ok {
				return bzip2.NewParallelReader(ra), nil
			}
			return bzip2.NewReader(in), nil
		} else {
			return nil, UnsupportedFormat{format}
		}
	}
	cmd := exec.Command(cmdPath, "-dc")
	cmd.Stdin = in
	cmd.Stderr = os.Stderr
	reader, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	cmd.Start()
	return NewFinishingReader(reader), nil
}
