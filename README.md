# Rick's Go concurrency package

[![Go Reference](https://pkg.go.dev/badge/github.com/rbranson/concurrent.svg)](https://pkg.go.dev/github.com/rbranson/concurrent)

## RCUArray

`RCUArray` is a generic read-copy-update array implementation that provides
non-blocking readers and a blocking writer with a precise grace period. Readers
call `Acquire` / `Release` pairs, while writers use `Swap` to publish new
values only after prior readers have quiesced. It supports allocating in strides
to avoid cache line contention from false sharing, which shows improvements of
3X in benchmarks.
