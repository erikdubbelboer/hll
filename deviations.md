Deviations
==========

In a few places, we made some changes/additions to the upstream HyperLogLog++ paper. These have been
kept to a minimum. Here they are.


Additions
---------
- We added a `Hll.Combine()` method that merges two HyperLogLog++ calculations, which gives you the
estimated cardinality of the union of their inputs. One of the benefits of HyperLogLog-type
algorithms is that they can be computed in parallel and merged in this way. The paper implied the
existence of this algorithm but didn't describe it.

- The `Merge()` and `DecodeSparse()` functions are described in a high-level way in the paper but not
specified in as much detail as the rest of the code. We think we've created algorithms that match
the authors' intent.

- In the paper, the algorithm has two separate phases. In the first phase, input elements are added.
In the second phase, cardinality is calculated. There's no notion of computing cardinality in the
middle of the input stream. Our algorithm avoids phases and allows cardinality to be computed while
new elements are still arriving by adding a simple tweak. At the top of the Cardinality() function, 
we merge the tmp_set and the sparse_list and check if a sparse->dense conversion is needed. If you 
don't do this, there's an edge case where the sparse_list could grow to an unbounded size: if you 
alternate calls to Add() and Cardinality(), the sparse->dense conversion will never occur.
