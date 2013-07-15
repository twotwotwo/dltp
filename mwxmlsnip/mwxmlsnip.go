// Public domain, Randall Farmer, 2013

package xmlsnip

import "bytes"

/*

SNIPPING

working from an incremental dump's XML, snip out non-final revisions and 
comment/contributor info to cut how much has to be encoded.

not used right now, and because some histories are GBs long, it's probably
better to do build snipping into SegmentReader.ReadNext, so a revision can be 
discarded as soon as we've determined it's not the final one.

*/

var revTag, commentTag, commentCloseTag, contributorTag, contributorCloseTag, nsTag []byte;

func snipBetween(in []byte, start []byte, end []byte) ([]byte) {
    startIdx := bytes.Index(in, start)
    endIdx := bytes.Index(in, end)
    if startIdx > -1 && endIdx > startIdx {
      endIdx += len(end)
      bytesCut := endIdx - startIdx
      copy(in[startIdx:], in[endIdx:])
      return in[:len(in) - bytesCut]
    }
    return in
}

// return any int at the start, or -1 if none found
func leadingInt(in []byte) (v int) {
  if len(in) == 0 {
    return -1
  }
  if in[0] < '0' || in[0] > '9' {
    return -1
  }
  for _, c := range in {
    if c < '0' || c > '9' {
      break
    }
    v *= 10
    v += int(byte(c) - byte('0'))
  }
  return v
}

func snipSegment(in []byte) []byte {
    // *destructively* snip all revisions but the last from input page XML
    if revTag == nil {
      revTag = []byte("<revision>")
      nsTag = []byte("<ns>")
      commentTag = []byte("<comment>")
      commentCloseTag = []byte("</comment>")
      contributorTag = []byte("<contributor>")
      contributorCloseTag = []byte("</contributor>")
    }
    
    // skip non-0 namespaces
    nsIdx := bytes.Index(in, nsTag)
    if nsIdx > -1 && leadingInt(in[nsIdx + len(nsTag):]) > 0 {
      return in[:0]
    }
    
    firstRevIdx := bytes.Index(in, revTag)
    lastRevIdx := bytes.LastIndex(in, revTag)
    if lastRevIdx > firstRevIdx {
      bytesCut := lastRevIdx - firstRevIdx
      copy(in[firstRevIdx:], in[lastRevIdx:])
      in = in[:len(in) - bytesCut]
    }

    in = snipBetween(in, commentTag, commentCloseTag)
    in = snipBetween(in, contributorTag, contributorCloseTag)
    return in
}
