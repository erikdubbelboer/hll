package hll

import (
	"math/bits"
	"sort"
)

const RHOW_BITS = 6
const RHOW_MASK = uint64(1<<RHOW_BITS) - 1

func encode(sparseIndex uint32, sparseRhoW uint8, p, pPrime uint) uint32 {
	mask := (uint32(1) << (pPrime - p)) - 1
	if (sparseIndex & mask) != 0 {
		return sparseIndex
	}

	var rhoEncodedFlag uint32
	if pPrime >= p+RHOW_BITS {
		rhoEncodedFlag = uint32(1) << pPrime
	} else {
		rhoEncodedFlag = uint32(1) << (p + RHOW_BITS)
	}

	normalIndex := sparseIndex >> (pPrime - p)

	return rhoEncodedFlag | normalIndex<<RHOW_BITS | uint32(sparseRhoW)
}

func computeRhoW(value uint64, bts uint8) uint8 {
	// Strip of the index and move the rhoW to a higher order.
	w := value << (64 - bts)

	// If the rhoW consists only of zeros, return the maximum length of bits + 1.
	if w == 0 {
		return bts + 1
	}
	return uint8(bits.LeadingZeros64(w) + 1)
}

// x is a hash code.
func encodeSparseHash(hash uint64, p, pPrime uint) (hashCode uint32) {
	sparseIndex := uint32(hash >> (64 - pPrime))
	sparseRhoW := computeRhoW(hash, uint8(64-pPrime))

	return encode(sparseIndex, sparseRhoW, p, pPrime)
}

// k is an encoded hash.
func decodeSparseHash(k uint64, p, pPrime uint) (idx uint64, rhoW uint8) {
	var rhoEncodedFlag uint64
	if pPrime >= p+RHOW_BITS {
		rhoEncodedFlag = uint64(1) << pPrime
	} else {
		rhoEncodedFlag = uint64(1) << (p + RHOW_BITS)
	}

	if k&rhoEncodedFlag == 0 {
		return k, 0
	}

	return ((k ^ rhoEncodedFlag) >> RHOW_BITS) << (pPrime - p), uint8(k & RHOW_MASK)
}

func decodeSparseHashForNormal(k uint64, p, pPrime uint) (idx uint64, rhoW uint8) {
	var rhoEncodedFlag uint64
	if pPrime >= p+RHOW_BITS {
		rhoEncodedFlag = uint64(1) << pPrime
	} else {
		rhoEncodedFlag = uint64(1) << (p + RHOW_BITS)
	}

	if k&rhoEncodedFlag == 0 {
		return k >> (pPrime - p), computeRhoW(k, uint8(pPrime-p))
	}

	return (k ^ rhoEncodedFlag) >> RHOW_BITS, uint8((k & RHOW_MASK) + uint64(pPrime) - uint64(p))
}

type mergeElem struct {
	index   uint64
	rho     uint8
	encoded uint64
}

type u64It func() (uint64, bool)

type mergeElemIt func() (mergeElem, bool)

func makeMergeElemIter(p, pPrime uint, input u64It) mergeElemIt {
	firstElem := true
	var lastIndex uint64
	return func() (mergeElem, bool) {
		for {
			hashCode, ok := input()
			if !ok {
				return mergeElem{}, false
			}
			idx, r := decodeSparseHash(hashCode, p, pPrime)
			if !firstElem && idx == lastIndex {
				// In the case where the tmp_set is being merged with the sparse_list, the tmp_set
				// may contain elements that have the same index value. In this case, they will
				// have been sorted so the one with the max rho value will come first. We should
				// discard all dupes after the first.
				continue
			}
			firstElem = false
			lastIndex = idx
			return mergeElem{idx, r, hashCode}, true
		}
	}
}

// The input should be sorted by hashcode.
func makeU64SliceIt(in []uint64) u64It {
	idx := 0
	return func() (uint64, bool) {
		if idx == len(in) {
			return 0, false
		}
		result := in[idx]
		idx++
		return result, true
	}
}

// Both input iterators must be sorted by hashcode.
func merge(p, pPrime uint, sizeEst uint64, it1, it2 u64It) *sparse {
	leftIt := makeMergeElemIter(p, pPrime, it1)
	rightIt := makeMergeElemIter(p, pPrime, it2)

	left, haveLeft := leftIt()
	right, haveRight := rightIt()

	output := newSparse(sizeEst)

	for haveLeft && haveRight {
		var toAppend uint64
		if left.index < right.index {
			toAppend = left.encoded
			left, haveLeft = leftIt()
		} else if right.index < left.index {
			toAppend = right.encoded
			right, haveRight = rightIt()
		} else { // The indexes are equal. Keep the one with the highest rho value.
			if left.rho > right.rho {
				toAppend = left.encoded
			} else {
				toAppend = right.encoded
			}
			left, haveLeft = leftIt()
			right, haveRight = rightIt()
		}
		output.Add(toAppend)
	}

	for haveRight {
		output.Add(right.encoded)
		right, haveRight = rightIt()
	}

	for haveLeft {
		output.Add(left.encoded)
		left, haveLeft = leftIt()
	}

	return output
}

func toNormal(s *sparse, p, pPrime uint) normal {
	m := 1 << p
	M := newNormal(uint64(m))

	it := s.GetIterator()
	for {
		k, ok := it()
		if !ok {
			break
		}
		idx, r := decodeSparseHashForNormal(k, p, pPrime)

		val := maxU8(M.Get(idx), r)
		M.Set(idx, val)
	}
	return M
}

func maxU8(x, y uint8) uint8 {
	if x >= y {
		return x
	}
	return y
}

func maxU64(x, y uint64) uint64 {
	if x >= y {
		return x
	}
	return y
}

func sortHashcodesByIndex(xs []uint64, p, pPrime uint) {
	sort.Sort(uint64Sorter{xs, p, pPrime})
}

type uint64Sorter struct {
	xs        []uint64
	p, pPrime uint
}

func (u uint64Sorter) Len() int {
	return len(u.xs)
}

func (u uint64Sorter) Less(i, j int) bool {
	iIndex, iRho := decodeSparseHash(u.xs[i], u.p, u.pPrime)
	jIndex, jRho := decodeSparseHash(u.xs[j], u.p, u.pPrime)
	if iIndex != jIndex {
		return iIndex < jIndex
	}
	return iRho > jRho
}

func (u uint64Sorter) Swap(i, j int) {
	u.xs[i], u.xs[j] = u.xs[j], u.xs[i]
}
