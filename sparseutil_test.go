package hll

import (
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/bmizerany/assert"
)

func TestSparseIterator(t *testing.T) {
	s := newSparse(5)
	inputs := []uint64{3, 5, 6, 6, 10}
	for _, x := range inputs {
		s.Add(x)
	}
	iter := s.GetIterator()
	for _, elem := range inputs {
		iterOutput, ok := iter()
		assert.T(t, ok)
		assert.Equal(t, uint64(elem), iterOutput)
	}
	_, ok := iter()
	assert.T(t, !ok) // iterator should be exhausted
}

func TestMerge(t *testing.T) {
	const p = 12
	const pPrime = 25

	convertToHashCodes := func(xs []uint64) {
		for i, x := range xs {
			xs[i] = uint64(encodeSparseHash(x, p, pPrime))
		}
	}

	rands1 := randUint64s(t, 200)
	convertToHashCodes(rands1)
	sortHashcodesByIndex(rands1, p, pPrime)
	input1 := makeU64SliceIt(rands1)

	rands2 := randUint64s(t, 100)
	convertToHashCodes(rands2)
	sortHashcodesByIndex(rands2, p, pPrime)
	input2 := makeU64SliceIt(rands2)

	merged := merge(p, pPrime, 0, input1, input2)

	var lastIndex uint64
	mergedIter := merged.GetIterator()
	value, valid := mergedIter()
	for valid {
		index, _ := decodeSparseHash(value, p, pPrime)
		assert.T(t, index > lastIndex, index, lastIndex)
		lastIndex = index
		value, valid = mergedIter()
	}
}

func randUint64s(t *testing.T, count int) []uint64 {
	output := make([]uint64, count)
	for i := 0; i < count; i++ {
		output[i] = randUint64(t)
	}
	return output
}

func randUint64(t *testing.T) uint64 {
	buf := make([]byte, 8)
	n, err := rand.Read(buf)
	assert.T(t, err == nil && n == 8, err, n)
	return binary.LittleEndian.Uint64(buf)
}

func TestDecodeHash(t *testing.T) {
	x, y := decodeSparseHash(0x4ce3e, 15, 20)
	if x != 314942 || y != 0 {
		t.Errorf("expected 314942, 0 got %d, %d", x, y)
	}

	x, y = decodeSparseHash(2097173, 15, 20)
	if x != 0 || y != 21 {
		t.Errorf("expected 0, 21 got %d, %d", x, y)
	}

	x, y = decodeSparseHash(18701, 14, 25)
	if x != 18701 || y != 0 {
		t.Errorf("expected 0, 21 got %d, %d", x, y)
	}
}

func TestEncode(t *testing.T) {
	x := encode(0, 21, 15, 20)
	if x != 2097173 {
		t.Errorf("expected\n0b%b got\n0b%b", 2097173, x)
	}

	r := computeRhoW(0xf00d, 64-20)
	if r != 29 {
		t.Errorf("expected %d got %d", 29, r)
	}

	x = encodeSparseHash(0xf00d, 15, 20)
	if x != 2097181 {
		t.Errorf("expected\n0b%b got\n0b%b", 314942, x)
	}

	r = computeRhoW(3226844164433860790, 64-20)
	if r != 1 {
		t.Errorf("expected %d got %d", 1, r)
	}

	x = encodeSparseHash(3226844164433860790, 15, 20)
	if x != 2464001 {
		t.Errorf("expected\n0b%b got\n0b%b", 2464001, x)
	}

	x = encodeSparseHash(0x2486bb7bf76b8a, 14, 25)
	if x != 18701 {
		t.Errorf("expected\n0b%b got\n0b%b", 18701, x)
	}
}
