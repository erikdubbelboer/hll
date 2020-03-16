package hll

// Bit manipulation functions

const all1s uint64 = 1<<64 - 1

// Return a bitmask containing ones from position startPos to endPos, inclusive.
// startPos and endPos are 0-indexed so they should be in [0,63].
// startPos should be less than or equal to endPos.
func onesFromTo(startPos, endPos uint) uint64 {
	// if endPos < startPos {
	// 	panic("assert")
	// }

	// Generate two overlapping sequences of 1s, and keep the overlap.
	highOrderOnes := all1s << startPos
	lowOrderOnes := all1s >> (64 - endPos - 1)
	result := highOrderOnes & lowOrderOnes
	return result
}
