HyperLogLog++ for Go
--------------------

This is a Go implementation of the HyperLogLog++ algorithm from "HyperLogLog in Practice:
Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm" by Heule,
Nunkesser and Hall of Google. This is a cardinality estimation algorithm: given a stream of input
elements, it will estimate the number of unique items in the stream. The estimation error can be
controlled by choosing how much memory to use. HyperLogLog++ improves on the basic HyperLogLog
algorithm by using less space, improving accuracy, and correcting bias.

This code is a translation of the pseudocode contained in Figures 6 and 7 of the Google paper.
Not all algorithms are provided in the paper, but we've tried our best to be true to the authors'
intent when writing the omitted algorithms. We're not trying to be creative, we're just
implementing the algorithm described in the paper as directly as possible. Our deviations are 
described [here](deviations.md).

The HyperLogLog++ paper is available [here](http://static.googleusercontent.com/media/research.google.com/en/us/pubs/archive/40671.pdf)

Instructions
------------

See the [docs](http://godoc.org/github.com/erikdubbelboer/hll).

Example of how to import data from Bigquery:
```go
import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/erikdubbelboer/hll"
	"google.golang.org/api/iterator"
)

func Import() (*hll.Hll, error) {
	bq, err := bigquery.NewClient(context.Background(), "project")
	if err != nil {
		return nil, fmt.Errorf("Can't connect to BigQuery: %w", err)
	}

	q := bq.Query(`
		SELECT
			HLL_COUNT.INIT(column) AS h,
		FROM project.dataset.table
		WHERE ...
	`)
	it, err := q.Read(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}

	var h *hll.Hll

	for {
		var r struct {
			H []byte `bigquery:"h"`
		}

		if err := it.Next(&r); err == iterator.Done {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to next: %w", err)
		}

		h, err = hll.NewHllFromBigquery(r.Users)
		if err != nil {
			return nil, err
		}
	}

	return h, nil
}
```
Adding a new value to a HLL imported from Bigquery:
```go
func Add(h *hll.Hll, val string) {
	h.Add(hll.BigQueryHash(val))
}
```
