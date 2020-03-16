package hll

import "testing"

func TestOnesTo(t *testing.T) {
	testCases := []struct {
		startPos, endPos uint
		expectResult     uint64
	}{
		{0, 0, 1},
		{63, 63, 1 << 63},
		{2, 4, 4 + 8 + 16},
		{56, 63, 0xFF00000000000000},
	}

	for i, testCase := range testCases {
		actualResult := onesFromTo(testCase.startPos, testCase.endPos)
		if testCase.expectResult != actualResult {
			t.Errorf("Case %d actual result was %v", i, actualResult)
		}
	}
}
