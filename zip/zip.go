// Public domain, Randall Farmer, 2013

package zip

import (
	"compress/bzip2"                   // ditto but uncompress only
	"compress/gzip"                    // fallback f/no pipeable gzip present (e.g., Windows)
	"github.com/twotwotwo/dltp/stream" // allow skipping fwd through streams
	"io"
	"os"
	"os/exec"
	"strings" // filename fun
)

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
  "gz": "pigz gzip",
  "bz2": "lbzip2 bzip2",
  "xz": "xz",
}

func Open(path string) (s stream.Stream, err error) {
	var file *os.File

  // try to open a raw file, then known compressed formats
  for _, suffix := range suffixes {
    file, err = os.Open(path + suffix)
    if err != nil {
	    if os.IsNotExist(err) {
		    continue
	    }
	    return nil, err
    }
    break
  }
  
  // didn't find it, sigh
  if file == nil {
    return nil, os.ErrNotExist
  }

	var compressedReader io.Reader
	
	for _, suffix := range suffixes {
	  if suffix == "" {
	    continue
	  }
	  if !strings.HasSuffix(file.Name(), suffix) {
	    continue
	  }
	  compressedReader, err = NewReader(file, suffix[1:])
	  if err != nil {
		  return nil, err
	  }
	  break
	}

  // return a Reader/ReaderAt, either file or wrapper
	if compressedReader == nil {
		return file, nil
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

func findZipper(format string) string {
	cmdPath, err := "", error(nil)

	choicesStr := programs[format]
	if choicesStr == "" {
		panic("unknown compression format " + format)
	}

	choices := strings.Split(choicesStr, " ")
	for _, cmd := range choices {
		cmdPath, err = exec.LookPath(cmd)
	}
	if err != nil {
	  panic("couldn't find (de)compressor for " + format + ": " + err.Error())
	}
	if cmdPath == "" {
	  panic("couldn't find (de)compressor for " + format)
	}
	
	return cmdPath
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

func NewReader(in io.Reader, format string) (r io.Reader, err error) {
	cmdPath := findZipper(format)
	if cmdPath == "" {
		if format == "gz" {
			return gzip.NewReader(in)
		} else if format == "bz2" {
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
	return reader, nil
}
