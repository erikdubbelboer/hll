package hll

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"

	"github.com/golang/protobuf/proto"
)

const DefaultBigqueryP = 15
const DefaultBigqueryPPrime = 20

func getField(buf *proto.Buffer) (uint64, uint64, error) {
	x, err := buf.DecodeVarint()
	if err != nil {
		return 0, 0, err
	}
	f := x >> 3 & 0b111
	t := x & 0b111
	return f, t, nil
}

func NewHllFromBigquery(data []byte) (*Hll, error) {
	if len(data) == 0 {
		return NewHll(DefaultBigqueryP, DefaultBigqueryPPrime), nil
	}

	var typ, numValues, encodingVersion, valueType, sparseSize, precision, sparsePrecision uint64
	var normalData, sparseData []byte

	buf := proto.NewBuffer(data)

	for len(data) > 0 {
		f, t, err := getField(buf)
		if err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return nil, err
		}

		if f == 1 {
			if t != 0 {
				return nil, fmt.Errorf("unexpected type: %d", t)
			}
			typ, err = buf.DecodeVarint()
			if err != nil {
				return nil, err
			}
		} else if f == 2 {
			if t != 0 {
				return nil, fmt.Errorf("unexpected type: %d", t)
			}
			numValues, err = buf.DecodeVarint()
			if err != nil {
				return nil, err
			}
		} else if f == 3 {
			if t != 0 {
				return nil, fmt.Errorf("unexpected type: %d", t)
			}
			encodingVersion, err = buf.DecodeVarint()
			if err != nil {
				return nil, err
			}
		} else if f == 4 {
			if t != 0 {
				return nil, fmt.Errorf("unexpected type: %d", t)
			}
			valueType, err = buf.DecodeVarint()
			if err != nil {
				return nil, err
			}
		} else {
			_, err := buf.DecodeVarint() // ignore size
			if err != nil {
				return nil, err
			}

			for len(data) > 0 {
				f, t, err := getField(buf)
				if err == io.ErrUnexpectedEOF {
					break
				} else if err != nil {
					return nil, err
				}

				if f == 2 { // SPARSE_SIZE_TAG
					if t != 0 {
						return nil, fmt.Errorf("unexpected type: %d", t)
					}
					sparseSize, err = buf.DecodeVarint()
					if err != nil {
						return nil, err
					}
				} else if f == 3 {
					if t != 0 {
						return nil, fmt.Errorf("unexpected type: %d", t)
					}
					precision, err = buf.DecodeVarint()
					if err != nil {
						return nil, err
					}
				} else if f == 4 {
					if t != 0 {
						return nil, fmt.Errorf("unexpected type: %d", t)
					}
					sparsePrecision, err = buf.DecodeVarint()
					if err != nil {
						return nil, err
					}
				} else if f == 5 {
					normalData, err = buf.DecodeRawBytes(true)
					if err != nil {
						return nil, err
					}
				} else if f == 6 {
					sparseData, err = buf.DecodeRawBytes(true)
					if err != nil {
						return nil, err
					}
				} else {
					fmt.Println(f, t)

					break
				}
			}

			break
		}
	}

	if typ != 112 {
		return nil, fmt.Errorf("unexpected type: %d", typ)
	}
	if encodingVersion != 2 {
		return nil, fmt.Errorf("unsupported encodingVersion: %d", encodingVersion)
	}

	h := NewHll(uint(precision), uint(sparsePrecision))
	if len(normalData) > 0 {
		if len(sparseData) > 0 {
			return nil, fmt.Errorf("cna not have both normal and sparse data")
		}

		h.switchToNormal()
		for idx, r := range normalData {
			h.bigM.Set(uint64(idx), r)
		}
	} else {
		h.sparseList.buf = sparseData
		h.sparseList.numElements = sparseSize

		it := h.sparseList.GetIterator()
		for {
			last, ok := it()
			if !ok {
				break
			}
			h.sparseList.lastVal = last
		}
	}

	// We ignore the number of values the aggregator has seen as we don't keep track of this.
	_ = numValues

	// We ignore valueType, it is the type of data that was put into the aggregator.
	// See: https://github.com/google/zetasketch/blob/a2f2692fae8cf61103330f9f70e696c4ba8b94b0/java/com/google/zetasketch/HyperLogLogPlusPlus.java#L442-L459
	_ = valueType

	return h, nil
}

const K0 uint64 = 0xa5b85c5e198ed849
const K1 uint64 = 0x8d58ac26afe12e47
const K2 uint64 = 0xc47b6e9e3a970ed3
const K3 uint64 = 0xc6a4a7935bd1e995

func load64(b []byte, offset int) uint64 {
	return binary.LittleEndian.Uint64(b[offset:])
}

func load64Safely(b []byte, offset, length int) uint64 {
	result := uint64(0)

	limit := 8
	if length < limit {
		limit = length
	}
	for i := 0; i < limit; i++ {
		// Shift value left while iterating logically through the array.
		result |= uint64(b[offset+i]&0xFF) << (i * 8)
	}
	return result
}

func rotateRight(x uint64, k int) uint64 {
	return bits.RotateLeft64(x, -k)
}

func shiftMix(val uint64) uint64 {
	return val ^ (val >> 47)
}

func hash128to64(high, low uint64) uint64 {
	a := (low ^ high) * K3
	a ^= (a >> 47)
	b := (high ^ a) * K3
	b ^= (b >> 47)
	b *= K3
	return b
}

func hashLength33To64(bytes []byte, offset, length int) uint64 {
	z := load64(bytes, offset+24)
	a := load64(bytes, offset) + (uint64(length)+load64(bytes, offset+length-16))*K0
	b := rotateRight(a+z, 52)
	c := rotateRight(a, 37)
	a += load64(bytes, offset+8)
	c += rotateRight(a, 7)
	a += load64(bytes, offset+16)
	vf := a + z
	vs := b + rotateRight(a, 31) + c
	a = load64(bytes, offset+16) + load64(bytes, offset+length-32)
	z = load64(bytes, offset+length-8)
	b = rotateRight(a+z, 52)
	c = rotateRight(a, 37)
	a += load64(bytes, offset+length-24)
	c += rotateRight(a, 7)
	a += load64(bytes, offset+length-16)
	wf := a + z
	ws := b + rotateRight(a, 31) + c
	r := shiftMix((vf+ws)*K2 + (wf+vs)*K0)
	return shiftMix(r*K0+vs) * K2
}

func murmurHash64WithSeed(bytes []byte, offset, length int, seed uint64) uint64 {
	mul := K3
	topBit := 0x7

	lengthAligned := length & (^topBit)
	lengthRemainder := length & topBit
	hash := seed ^ (uint64(length) * mul)

	for i := 0; i < lengthAligned; i += 8 {
		loaded := load64(bytes, offset+i)
		data := shiftMix(loaded*mul) * mul
		hash ^= data
		hash *= mul
	}

	if lengthRemainder != 0 {
		data := load64Safely(bytes, offset+lengthAligned, lengthRemainder)
		hash ^= data
		hash *= mul
	}

	hash = shiftMix(hash) * mul
	hash = shiftMix(hash)
	return hash
}

/**
 * Computes intermediate hash of 32 bytes of byte array from the given offset. Results are
 * returned in the output array - this is 12% faster than allocating new arrays every time.
 */
func weakHashLength32WithSeeds(bytes []byte, offset int, seedA, seedB uint64) [2]uint64 {
	part1 := load64(bytes, offset)
	part2 := load64(bytes, offset+8)
	part3 := load64(bytes, offset+16)
	part4 := load64(bytes, offset+24)

	seedA += part1
	seedB = rotateRight(seedB+seedA+part4, 51)
	c := seedA
	seedA += part2
	seedA += part3
	seedB += rotateRight(seedA, 23)

	return [2]uint64{seedA + part4, seedB + c}
}

/*
 * Compute an 8-byte hash of a byte array of length greater than 64 bytes.
 */
func fullFingerprint(bytes []byte, offset, length int) uint64 {
	// For lengths over 64 bytes we hash the end first, and then as we
	// loop we keep 56 bytes of state: v, w, x, y, and z.
	x := load64(bytes, offset)
	y := load64(bytes, offset+length-16) ^ K1
	z := load64(bytes, offset+length-56) ^ K0
	v := weakHashLength32WithSeeds(bytes, offset+length-64, uint64(length), y)
	w := weakHashLength32WithSeeds(bytes, offset+length-32, uint64(length)*K1, K0)
	z += shiftMix(v[1]) * K1
	x = rotateRight(z+x, 39) * K1
	y = rotateRight(y, 33) * K1

	// Decrease length to the nearest multiple of 64, and operate on 64-byte chunks.
	length = (length - 1) & ^63
	for {
		x = rotateRight(x+y+v[0]+load64(bytes, offset+16), 37) * K1
		y = rotateRight(y+v[1]+load64(bytes, offset+48), 42) * K1
		x ^= w[1]
		y ^= v[0]
		z = rotateRight(z^w[0], 33)
		v = weakHashLength32WithSeeds(bytes, offset, v[1]*K1, x+w[0])
		w = weakHashLength32WithSeeds(bytes, offset+32, z+w[1], y)
		tmp := z
		z = x
		x = tmp
		offset += 64
		length -= 64

		if length == 0 {
			break
		}
	}
	return hash128to64(hash128to64(v[0], w[0])+shiftMix(y)*K1+z, hash128to64(v[1], w[1])+x)
}

func BigQueryHash(s string) uint64 {
	bytes := []byte(s)
	offset := 0
	length := len(bytes)
	var result uint64

	if length <= 32 {
		result = murmurHash64WithSeed(bytes, offset, length, K0^K1^K2)
	} else if length <= 64 {
		result = hashLength33To64(bytes, offset, length)
	} else {
		result = fullFingerprint(bytes, offset, length)
	}

	u := K0
	if length >= 8 {
		u = load64(bytes, offset)
	}
	v := K0
	if length >= 9 {
		v = load64(bytes, offset+length-8)
	}
	result = hash128to64(result+v, u)
	if result == 0 || result == 1 {
		return result + ^uint64(1)
	}
	return result
}
