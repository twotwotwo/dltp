// Public domain, Randall Farmer, 2013

package zip

import (
    "os"
    "io"
    "strings"        // filename fun
    "os/exec"
    "compress/gzip"  // fallback f/no pipeable gzip present (e.g., Windows)
    "compress/bzip2" // ditto but uncompress only
)

/*

(UN)ZIP HELPER

Open(path string):
given path "a.xml", open a.xml, a.xml.gz, or a.xml.bz2, and either pipe via
a native decompressor or use go's own. either way, return a Reader.

NewWriter/NewReader:
pipe through a native compressor (pigz, gzip; you can imagine (l)bzip2, too)
or use go's own gzip

*/

func Open(path string) (reader io.Reader, err error) {
    suffixes := []string{"", ".gz", ".bz2", ".xz"}
    var file *os.File
    for _, suffix := range suffixes {
    		file, err = os.Open(path + suffix)
    		if err != nil {
    		    if os.IsNotExist(err) {
    		        continue
    		    }
    		    return nil, err
    		}
    		reader = file
        break
    }
    if file == nil {
        return nil, os.ErrNotExist
    }

    if strings.HasSuffix(file.Name(), ".gz") {
        reader, err = NewReader(file, "gz")
        if err != nil {
            return nil, err
        }
    }
    if strings.HasSuffix(file.Name(), ".bz2") {
        reader, err = NewReader(file, "bz2")
        if err != nil {
            return nil, err
        }
    }
    if strings.HasSuffix(file.Name(), ".xz") {
        reader, err = NewReader(file, "xz")
        if err != nil {
            return nil, err
        }
    }

    return
}

type CmdPipe struct {
    cmd *exec.Cmd
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
    if format == "bz2" {
        cmdPath, err = exec.LookPath("lbzip2")
        if err != nil {
            cmdPath, err = exec.LookPath("bzip2")
        }
    } else if format == "gz" {
        cmdPath, err = exec.LookPath("pigz")
        if err != nil {
            cmdPath, err = exec.LookPath("gzip")
        }
    } else if format == "xz" {
        cmdPath, err = exec.LookPath("xz")
    } else {
        panic("unknown compression format " + format)
    }
    return cmdPath
}

func CanWrite(format string) bool {
    if format == "gz" {
        return true
    }
    return findZipper(format) != ""
}

func NewWriter(out io.Writer, format string) (io.WriteCloser) {
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


