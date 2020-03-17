package hll

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"strconv"
)

func Example() {
	const (
		p           = 14 // Max memory usage is 0.75 * 2^p bytes
		pPrime      = 25 // Setting this is a bit more complicated, Google recommends 25.
		numToInsert = 1000000
	)

	// You can use any good hash function, truncated to 8 bytes to fit in a uint64.
	hashU64 := func(s string) uint64 {
		sha1Hash := sha1.Sum([]byte(s))
		return binary.LittleEndian.Uint64(sha1Hash[0:8])
	}

	hll := NewHll(p, pPrime)

	// For this example, our inputs will just be strings, e.g. "1", "2"
	for i := 0; i < numToInsert; i++ {
		inputString := strconv.Itoa(i)
		hash := hashU64(inputString)

		// To use HLL, you hash your item, convert the hash to uint64, and pass it to Add().
		hll.Add(hash)
	}

	// Duplicates do not affect the cardinality. The following loop has no effect.
	for i := 0; i < 10000; i++ {
		hll.Add(hashU64("1"))
	}

	// We inserted 1M unique elements, the cardinality should be roughly 1M.
	fmt.Printf("%d\n", hll.Cardinality())
	// Output: 1010201
}
