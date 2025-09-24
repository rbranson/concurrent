package concurrent

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkSwapContention(b *testing.B) {
	workers := runtime.GOMAXPROCS(0)
	cases := []struct {
		name   string
		stride int
	}{
		{name: "Stride256", stride: 256},
		{name: "Stride128", stride: 128},
		{name: "Stride64", stride: 64},
		{name: "Stride32", stride: 32},
		{name: "Stride0", stride: 0},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			ra := NewRCUArray[int](workers, tc.stride)
			var workerSeq atomic.Uint64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				workerID := int(workerSeq.Add(1) - 1)
				idx := workerID % workers

				value := workerID
				for pb.Next() {
					value++
					_ = ra.Swap(idx, value)
				}
			})
		})
	}
}

func BenchmarkSwapContentionCompare(b *testing.B) {
	workers := runtime.GOMAXPROCS(0)
	cases := []struct {
		name   string
		stride int
		impl   string
	}{
		{name: "Stride0/RCU", stride: 0, impl: "RCU"},
		{name: "Stride0/RWMutex", stride: 0, impl: "RWMutex"},
		{name: "Stride256/RCU", stride: 256, impl: "RCU"},
		{name: "Stride256/RWMutex", stride: 256, impl: "RWMutex"},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			switch tc.impl {
			case "RCU":
				benchmarkSwapRCU(b, workers, tc.stride)
			case "RWMutex":
				benchmarkSwapRWMutex(b, workers, tc.stride)
			default:
				b.Fatalf("unsupported implementation %q", tc.impl)
			}
		})
	}
}

func benchmarkSwapRCU(b *testing.B, workers, stride int) {
	ra := NewRCUArray[int](workers, stride)
	var workerSeq atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(workerSeq.Add(1) - 1)
		idx := workerID % workers

		value := workerID
		for pb.Next() {
			value++
			_ = ra.Swap(idx, value)
		}
	})
}

func benchmarkSwapRWMutex(b *testing.B, workers, stride int) {
	var getSlot func(int) (*sync.RWMutex, *int)

	switch stride {
	case 0:
		type slot struct {
			mu    sync.RWMutex
			value int
		}
		slots := make([]slot, workers)
		getSlot = func(i int) (*sync.RWMutex, *int) {
			s := &slots[i]
			return &s.mu, &s.value
		}
	case 256:
		type slot struct {
			mu    sync.RWMutex
			value int
			_     [256]byte // padding
		}
		slots := make([]slot, workers)
		getSlot = func(i int) (*sync.RWMutex, *int) {
			s := &slots[i]
			return &s.mu, &s.value
		}
	default:
		b.Fatalf("unsupported stride %d", stride)
	}

	var workerSeq atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(workerSeq.Add(1) - 1)
		idx := workerID % workers
		mu, valuePtr := getSlot(idx)

		value := workerID
		for pb.Next() {
			value++
			mu.Lock()
			prev := *valuePtr
			*valuePtr = value
			mu.Unlock()
			_ = prev
		}
	})
}
