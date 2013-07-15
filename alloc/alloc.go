// Public domain, Randall Farmer, 2013

package alloc

func Bytes(buf []byte, size int) []byte {
	finalCap := cap(buf)
	if finalCap == 0 {
	  finalCap = 2e6
	}
	for size > finalCap {
		finalCap *= 2
	}
	if finalCap > cap(buf) {
		return make([]byte, size, finalCap)
	}
	return buf[:size]
}

func CopyBytes(dst []byte, src []byte) []byte {
  dst = Bytes(dst, len(src))
	return dst[:copy(dst, src)]
}


