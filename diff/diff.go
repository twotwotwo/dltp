// Public domain, Randall Farmer, 2013

package diff

import (
  "encoding/binary"
  "bytes"
  "io"
  "bufio"
  "math/rand"
  "github.com/twotwotwo/dltp/alloc"
)

/*

DIFFING

model: all operations either copy bytes from "a" (the reference) or insert 
literal bytes in to "b" (the new revision). there's a "cursor" value tracking
the last position in a, and copies near the cursor position have shorter 
encodings.

format: 

each instruction starts with a signed variable-length integer (for spec see 
https://developers.google.com/protocol-buffers/docs/encoding).

positive values represent literals: the value is the number of literal bytes 
that should be copied to the stream. 

negative values represent copies: the value is the number of bytes to be 
copied. 

zero means the diff has ended. so diffs are self-delimiting: you don't have
to write the length to the file. also, you can tell if a diff has been 
truncated, as patchFromReader does.

instructions are mixed in with the data, as in, say, the Git packfile format 
(see https://github.com/git/git/blob/master/diff-delta.c and 
http://stackoverflow.com/questions/9478023/is-the-git-binary-diff-algorithm-delta-storage-standardized). 
rzip separates out the instructions, which may help the zipper's compression 
ratio.

usage, roughly:
  s := MatchState{a: originalBytes, b: revisedBytes}
  s.Diff()
  s.Out.WriteTo(os.Stdout)

other fields of matchState store stuff like the hash table (h), the minimum 
value in it (base), the "current position" in a during the match (cursor), 
and a mask (hMask) indicating which bits of the rolling hash have to be 1 for
the offset to be put in the hashtable.

the rolling hash is simple (XOR in an entry from a list of 256), and a lot like 
rzip's. the hash table has 64K entries after some basic testing.

MatchStates can be reused to save allocations.

also, maybe more important, wouldn't be that crazy to scrap the whole thing for
Git's code or the like

*/

type hVal uint32
var hMax hVal = 0xFFFFFFFF

type MatchState struct {
	A       []byte
	B       []byte
	h       []hVal
	base    hVal
	hMask   uint64
	hBits   uint64
	cursor  int
	Out     *bytes.Buffer
	active  bool // dumb race-condition detection
	encBuf  [20]byte
}

func (s *MatchState) putLiteral(start int, end int) {
	if end == start {
		panic("Tried to write a zero-length literal; shouldn't happen")
	}
	inst := s.encBuf[:]
	i := binary.PutVarint(inst, int64(end-start))
	_, err := s.Out.Write(inst[:i])
	if err != nil {
		panic("failed to write literal length")
	}
	_, err = s.Out.Write(s.B[start:end])
	if err != nil {
		panic("failed to write literal content")
	}
	s.cursor += end - start
	s.B = s.B[end:]
	return
}

func (s *MatchState) putCopy(start int, end int) {
	if end == start {
		panic("Tried to write a zero-length copy; shouldn't happen")
	}
	inst := s.encBuf[:]
	i := binary.PutVarint(inst, -int64(end-start))
	j := binary.PutVarint(inst[i:], int64(start-s.cursor))
	_, err := s.Out.Write(inst[:i+j])
	if err != nil {
		panic("failed to write copy instruction")
	}
	s.cursor = end
	return
}

var zarro [1]byte

func (s *MatchState) putEnd() {
	_, err := s.Out.Write(zarro[:])
	if err != nil {
		panic("failed to write end-of-diff marker")
	}
}

func makeBuzTbl() []uint64 {
    buzTbl := make([]uint64, 256)
    for i := 0; i < 256; i++ {
        buzTbl[i] = uint64(rand.Int63()) // sigh
        // put something in the top bit
        buzTbl[i] ^= buzTbl[i] << 4
    }
    return buzTbl
}

var buzTbl []uint64 = makeBuzTbl()

var hashSz = 131072
var hMinMatch = 8

// return a mask for filtering out a portion of hashes
func hashMask(sz int, hSz int) uint64 {
    // bytes per table entry
    ratio := sz / hashSz
    r := uint64(ratio * 2) // let's 1/2 fill the hash table
    i := 0
    for r > 0 {
        r >>= 1
        i++
    }
    return (uint64(1<<uint(i))-1) << uint(64-i)
}

func (s *MatchState) hash(a []byte, offs hVal) {
    if len(a) < hMinMatch { // nothin' we can do for ya
        s.h = s.h[0:]
        return
    }
    
    base := s.base
    
    h := s.h[:]
    if h == nil {
        h = make([]hVal, hashSz)
    } else {
      if hMax - hVal(len(a)) < s.base {
          s.base = 0
          base = 0
          for i := 0; i < hashSz; i++ {
              h[i] = 0
          }
      }
    }
    
    var v uint64
    for i := 0; i < hMinMatch; i++ {
        v ^= buzTbl[a[i]]
    }

    hBits := uint64(hashSz - 1)
    hMask := hashMask(hashSz, len(a))
    lenA := len(a)
    for i := hMinMatch; i < lenA; i++ {
        v ^= buzTbl[a[i]] ^ buzTbl[a[i - hMinMatch]]
        if v & hMask != hMask {
            continue
        }
        h[v & hBits] = hVal(i) + base + offs
    }
    
    // save to matchState (meh; should maybe be own class)
    s.h = h
    s.hMask = hMask
    s.hBits = hBits
}

func matchAround(a []byte, b []byte, iAStart int, iBStart int) (aStart int, bStart int, l int) {
    aStart, bStart = iAStart, iBStart
    l = 0
    for aStart >= 0 && bStart >= 0 && a[aStart] == b[bStart] {
      aStart--
      bStart--
      l++
    }
    aStart++; bStart++; l--; // loop goes one too far
    lA, lB := len(a), len(b)
    lMax := lA - aStart
    lMaxB := lB - bStart
    if lMaxB < lMax {
        lMax = lMaxB
    }
    for l < lMax && a[aStart + l] == b[bStart + l] {
      l++
    }
    return
}

func (s *MatchState) match() {
    a, b := s.A, s.B
    base := s.base
    h, hBits, hMask := s.h, s.hBits, s.hMask

    for {
        // init hash for b
        if len(b) <= hMinMatch || len(h) == 0 {
            if len(b) > 0 {
                s.B = b
                s.putLiteral(0, len(b))
            }
            //fmt.Println("survived writing literal.")
            return
        }
        var v uint64
        //fmt.Println("initing hash")
        for i := 0; i < hMinMatch; i++ {
            v ^= buzTbl[b[i]]
        }
        
        // step through b for a match
        matchSuccess := false
        //fmt.Println("hashing the rest")
        for i := hMinMatch; i < len(b); i++ {
            // Find a match in the hashtable
            v ^= buzTbl[b[i]] ^ buzTbl[b[i - hMinMatch]]
            if v & hMask != hMask {
                continue
            }
            hVal := h[v & hBits]
            if hVal < base {
                continue
            }
            aStart := int(hVal - base)
            if aStart > len(a) {
                panic("aStart was high")
            }

            // Get the full extent of the match
            l, bStart := 0, i
            for aStart >= 0 && bStart >= 0 && a[aStart] == b[bStart] {
              aStart--
              bStart--
              l++
            }
            aStart++; bStart++; l--; // loop goes one too far
            lA, lB := len(a), len(b)
            lMax := lA - aStart
            lMaxB := lB - bStart
            if lMaxB < lMax {
                lMax = lMaxB
            }
            for l < lMax && a[aStart + l] == b[bStart + l] {
              l++
            }
            
            if l < hMinMatch { // too short
                continue
            }
            
            s.B = b // why?
            if bStart > 0 {
                s.putLiteral(0, bStart)
            }
            s.putCopy(aStart, aStart+l)
            b = b[bStart+l:]
            matchSuccess = true
            break
        }

        if !matchSuccess { // zadness.
            //fmt.Println("sadness, no match!")
            s.B = b
            s.putLiteral(0, len(b))
            return
        }

    }
}

func (s *MatchState) Diff() {
  if s.active {
    panic("two users, one MatchState")
  }
  s.active = true
  s.cursor = 0

  aStart, bStart := 0,0

  s.hash(s.A[aStart:], hVal(aStart))
  s.B = s.B[bStart:]
	s.match()
	
	s.base += hVal(len(s.A))

	s.putEnd()
	s.active = false
}

// not parallel-safe, but decoding is not parallel
var literalBuf, outBuf []byte 
func Patch(a []byte, diff *bufio.Reader) []byte {
	// panicing here is not very go-native-y
	cursor := 0
	literalBuf = alloc.Bytes(literalBuf, 5e5)[:0]
	outBuf = alloc.Bytes(outBuf, len(a))[:0]
	for {
		instrFirst64, err := binary.ReadVarint(diff)
		if err != nil {
			if err == io.EOF {
				panic("Truncated diff")
			}
			panic(err)
		}
		instrFirst := int(instrFirst64)  // 386: lengths can only be 2GB (OK)
		if instrFirst > 0 { // literal
			literalLen := instrFirst
			literalBuf = alloc.Bytes(literalBuf, literalLen)
			_, err := io.ReadFull(diff, literalBuf)
			if err != nil {
				if err == io.EOF {
					panic("Literal length was more than content available (file truncated or was not a diff?)")
				}
				panic(err)
			}
			outBuf = append(outBuf, literalBuf...)
			cursor += literalLen // move fwd in a as well
		} else if instrFirst == 0 {
			return outBuf // valid end of diff
		} else { // copy (indicated by negative sign)
			copyLen := -instrFirst
			copyMove64, err := binary.ReadVarint(diff)
			if err != nil {
				if err == io.EOF {
					panic("copy instruction truncated, weird")
				}
				panic(err)
			}
			copyMove := int(copyMove64) // 386: copies can only move 2GB (OK)
			cursor += copyMove
			if cursor < 0 {
				panic("Copy would start before start of source")
			}
			if cursor > len(a) {
				panic("Copy would start after end of source--truncated source?")
			}
			if cursor+copyLen > len(a) {
				panic("Copy would end after end of source--truncated source?")
			}
			outBuf = append(outBuf, a[cursor:cursor+copyLen]...)
			cursor += copyLen
		}
	}
}


