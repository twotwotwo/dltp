// Public domain, Randall Farmer, 2013

package scan

import (
	"bytes"
	"io"
)

/*

SCANNING

read through a stream up to a given string, and peek at integers

*/

type Scanner struct {
	in io.Reader
	// These start with the first unconsumed char
	unread     []byte
	unreadOffs int64
	// These cover everything since last Discard()
	All  []byte
	Offs int64
	// And this covers everything allocated
	backing []byte
}

// fill s.All with more data--return bytes read in, or -1 if no data was
// available. may expand the buffer or move data around in it.
func (s *Scanner) fill() int64 {
	if len(s.All) == cap(s.All) {
		old := s.All
		s.All = make([]byte, len(s.All), cap(s.All)*2)
		s.backing = s.All
		copy(s.All, old)
	}
	c, err := s.in.Read(s.All[len(s.All):cap(s.All)])
	s.All = s.All[:len(s.All)+c]
	s.unread = s.All[s.unreadOffs-s.Offs:]
	if err != nil {
		if err != io.EOF {
			panic(err)
		}
		if c == 0 {
			return -1
		}
	}
	return int64(c)
}

// mark data read
func (s *Scanner) consume(length int) { // 386: segments need to be <2GB (OK)
	s.unread = s.unread[length:]
	s.unreadOffs += int64(length)
}

// discard
func (s *Scanner) Discard() {
	length := len(s.unread)
	if cap(s.All) < cap(s.backing)/2 {
		copy(s.backing[:length], s.unread)
		s.All = s.backing[:length]
	} else { // avoid copy
		s.All = s.All[s.unreadOffs-s.Offs:]
	}
	s.Offs = s.unreadOffs
	s.All = s.All[:length]
	s.unread = s.All
}

func (s *Scanner) ScanTo(a []byte, inclusive bool, discard bool) int64 {
	i := bytes.Index(s.unread, a)
	for i == -1 {
		if len(s.unread) > (len(a) - 1) {
			s.consume(len(s.unread) - (len(a) - 1))
			if discard {
				s.Discard()
			}
		}
		c := s.fill()
		if c == -1 {
			// consume everything
			s.consume(len(s.unread))
			if discard {
				s.Discard()
			}
			return c
		}
		i = bytes.Index(s.unread, a)
	}
	if inclusive {
		i += len(a)
	}
	s.consume(i)
	return s.unreadOffs
}

// look for whichever of a set of sequences comes up first in the stream.
// we depend on this returning the same byte slice passed in.
func (s *Scanner) ScanToAny(aChoices [][]byte, inclusive bool, discard bool) (int64, []byte) {
	i := -1
	a := []byte(nil)
	overlap := 0

	for {
		// look for the sequence that appears first in the buffer
		for _, myA := range aChoices {
			myI := bytes.Index(s.unread, myA)
			// not the most elegant if cond
			if myI != -1 && (i == -1 || myI < i) {
				i = myI
				a = myA
			}
		}
		// if found, return where we ended up
		if i != -1 {
			if inclusive {
				i += len(a)
			}
			s.consume(i)
			return s.unreadOffs, a
		}

		// determine how much overlap to keep, then fill buffer
		if overlap == 0 {
			for _, myA := range aChoices {
				if len(myA)-1 > overlap {
					overlap = len(myA) - 1
				}
			}
		}
		if len(s.unread) > overlap {
			s.consume(len(s.unread) - overlap)
			if discard {
				s.Discard()
			}
		}
		c := s.fill()

		// bail out if there is no more data to read
		if c == -1 {
			// consume everything
			s.consume(len(s.unread))
			if discard {
				s.Discard()
			}
			return c, nil
		}

	}
}

/*
 * consumeLimited and LimitedScan are for a so-far-hypothetical mode where we
 * compress an *entire* revision history but break it into largish chunks (say,
 * 10MB at a go) to save memory. tl;dr: not used.
 */

// consume bytes, respecting a limit,
func (s *Scanner) consumeLimited(bytes int, limit int) (consumed int) {
	if bytes < limit {
		s.consume(bytes)
		return bytes
	} else {
		s.consume(limit)
		return limit
	}
}

// scan for a string, but don't consume more than a certain amount. handy
// for splitting big revision histories into chunks we can handle.
//
// if it's more convenient at the higher levels, 'found' could become
// 'notLimited' and be true at EOF
func (s *Scanner) LimitedScan(a []byte, maxDistance int, inclusive bool) (off int64, found bool) {
	remainingDistance := maxDistance
	i := bytes.Index(s.unread, a)
	for i == -1 {
		if len(s.unread) > (len(a) - 1) {
			// eat what we scanned (leaving a bit in case substring straddles a chunk
			// boundary), and bail w/failure if we've read our limit
			remainingDistance -=
				s.consumeLimited(len(s.unread)-(len(a)-1), remainingDistance)
			if remainingDistance <= 0 {
				return s.unreadOffs, false
			}
		}
		c := s.fill()
		if c == -1 {
			// eat the last bytes, and bail w/-1 and failure
			remainingDistance -=
				s.consumeLimited(len(s.unread), remainingDistance)
			return c, false
		}
		i = bytes.Index(s.unread, a)
	}
	if inclusive {
		i += len(a)
	}
	remainingDistance -=
		s.consumeLimited(i, remainingDistance)
	if remainingDistance < 0 { // == 0 means it was right at end of allowed span
		return s.unreadOffs, false
	} else {
		return s.unreadOffs, true
	}
}

func (s *Scanner) ReadBytes(a []byte) (res []byte) {
	res = append(a[:0], s.All[:s.unreadOffs-s.Offs]...)
	s.Discard()
	return
}

func (s *Scanner) ReadString() (res string) {
	res = string(s.All[:s.unreadOffs-s.Offs])
	s.Discard()
	return
}

// reads an unsigned int into a signed int type; -1 if there's no int there
// consumes nothing (hence Peek), and may read data
func (s *Scanner) PeekInt() (parsed int) {
	// ensure the longest int we can expect fits in buffer
	if len(s.unread) < 21 {
		i := s.fill()
		// -1 is not special here; int at EOF is ok

		// corner case: there was space in s.All, but not enough for our int
		if i != -1 && cap(s.unread) < 21 {

			i = s.fill() // a second fill() should double the size of s.All

			// *real* corner case: we started with too small an s.All
			if i != -1 && cap(s.unread) < 21 {
				panic("initial read buffer needs to be a reasonable size (say, 10kb)")
			}

		}

	}
	// cheap atoi; doesn't recognize too-large ints, floats, 1e6, and so on
	if len(s.unread) == 0 {
		return -1
	}
	for i, c := range s.unread {
		if c < byte('0') || c > byte('9') || i == 21 {
			if i == 0 {
				return -1
			}
			return parsed
		}
		parsed *= 10
		parsed += int(c - byte('0')) // 386: ns/id max out at 2GB (OK)
	}
	return parsed // int at EOF is OK
}

func NewScanner(r io.Reader, cap int) (s *Scanner) {
	buf := make([]byte, 0, cap)
	s = &Scanner{
		in:      r,
		All:     buf,
		backing: buf,
		unread:  buf,
	}
	s.fill()
	return
}

func (s *Scanner) Content() []byte {
	return s.All[:s.unreadOffs-s.Offs] // then caller should usually Discard
}
