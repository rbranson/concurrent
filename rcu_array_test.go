package concurrent

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRCUArrayLen(t *testing.T) {
	cases := []int{0, 1, 2, 7, 64, 127, 256}
	for _, n := range cases {
		ra := NewRCUArray[int](n)
		if ra.Len() != n {
			t.Fatalf("Len() = %d, want %d", ra.Len(), n)
		}
		if ra.Length() != n {
			t.Fatalf("Length() = %d, want %d", ra.Length(), n)
		}
	}
}

func TestRCUArray_Basic(t *testing.T) {
	ra := NewRCUArrayFromSlice([]int{1, 2, 3})
	if got, want := ra.Len(), 3; got != want {
		t.Fatalf("Len()=%d want %d", got, want)
	}

	v := ra.Acquire(1)
	if v != 2 {
		t.Fatalf("Acquire got %d want 2", v)
	}
	ra.Release(1)

	prev := ra.Swap(1, 9)
	if prev != 2 {
		t.Fatalf("Swap returned %d want 2", prev)
	}
	v2 := ra.Acquire(1)
	if v2 != 9 {
		t.Fatalf("Acquire after swap got %d want 9", v2)
	}
	ra.Release(1)
}

func TestRCUArray_SizeAndStride(t *testing.T) {
	n := 5
	// Default compact (stride 0 -> packed)
	ra0 := NewRCUArray[int](n)
	size0 := ra0.Size()
	if size0 == 0 {
		t.Fatalf("Size() should be > 0 for n=%d", n)
	}

	// Explicit compact via WithStride(0) should be comparable or equal
	ra0b := NewRCUArrayWithStride[int](n, 0)
	size0b := ra0b.Size()
	if size0b != size0 {
		t.Fatalf("Size() mismatch: stride0a=%d stride0b=%d", size0, size0b)
	}

	// Stride >= slot size should increase backing at least by (stride-slotSize)*n approximately
	ra64 := NewRCUArrayWithStride[int](n, 64)
	size64 := ra64.Size()
	if size64 <= size0 {
		t.Fatalf("Size() with stride 64 should be > compact: %d <= %d", size64, size0)
	}

	// Stride smaller than slot size should be accepted and rounded up
	underStride := NewRCUArrayWithStride[int](n, 32)
	if got := underStride.Size(); got != size0 {
		t.Fatalf("Size() with stride smaller than slot size: got %d want %d", got, size0)
	}

	// Verify functionality works with configured stride
	prev := ra64.Swap(2, 42)
	if prev != 0 {
		t.Fatalf("Swap returned %d want 0", prev)
	}
	if got := ra64.Get(2); got != 42 {
		t.Fatalf("Get(2)=%d want 42", got)
	}
}

func TestRCUArray_StrideConsistency(t *testing.T) {
	strides := []int{0, 32, 64, 128, 256}
	n := 9

	for _, stride := range strides {
		stride := stride
		t.Run(fmt.Sprintf("stride_%d", stride), func(t *testing.T) {
			values := make([]int, n)
			for i := range values {
				values[i] = (stride+1)*1_000_000 + i*7 + 3
			}

			ra := NewRCUArrayFromSliceWithStride[int](values, stride)
			for i := 0; i < n; i++ {
				if got := ra.Get(i); got != values[i] {
					t.Fatalf("Get(%d)=%d want %d", i, got, values[i])
				}
				v := ra.Acquire(i)
				if v != values[i] {
					t.Fatalf("Acquire(%d)=%d want %d", i, v, values[i])
				}
				ra.Release(i)
			}

			updated := make([]int, n)
			for i := range updated {
				updated[i] = -values[i] - 17
			}

			for i := 0; i < n; i++ {
				prev := ra.Swap(i, updated[i])
				if prev != values[i] {
					t.Fatalf("Swap(%d) returned %d want %d", i, prev, values[i])
				}
			}

			for i := 0; i < n; i++ {
				if got := ra.Get(i); got != updated[i] {
					t.Fatalf("Get(%d) after swap=%d want %d", i, got, updated[i])
				}
				v := ra.Acquire(i)
				if v != updated[i] {
					t.Fatalf("Acquire(%d) after swap=%d want %d", i, v, updated[i])
				}
				ra.Release(i)
			}
		})
	}
}

func TestRCUArray_GetBasic(t *testing.T) {
	ra := NewRCUArrayFromSlice([]int{10, 20, 30})
	if got, want := ra.Get(0), 10; got != want {
		t.Fatalf("Get(0)=%d want %d", got, want)
	}
	if got, want := ra.Get(2), 30; got != want {
		t.Fatalf("Get(2)=%d want %d", got, want)
	}
	prev := ra.Swap(1, 99)
	if prev != 20 {
		t.Fatalf("Swap returned %d want 20", prev)
	}
	if got, want := ra.Get(1), 99; got != want {
		t.Fatalf("Get(1) after swap=%d want %d", got, want)
	}
}

func TestRCUArray_SwapWaitsForAcquires(t *testing.T) {
	ra := NewRCUArrayFromSlice([]int{7})
	var started sync.WaitGroup
	started.Add(1)

	// Hold an acquisition to force Swap to wait.
	go func() {
		_ = ra.Acquire(0)
		started.Done()
		time.Sleep(50 * time.Millisecond)
		ra.Release(0)
	}()
	started.Wait()

	t0 := time.Now()
	prev := ra.Swap(0, 8)
	dt := time.Since(t0)
	if prev != 7 {
		t.Fatalf("Swap returned %d want 7", prev)
	}
	if dt < 45*time.Millisecond { // should have waited roughly for the held acquisition
		t.Fatalf("Swap did not wait, duration=%v", dt)
	}

	v := ra.Acquire(0)
	if v != 8 {
		t.Fatalf("Acquire after swap got %d want 8", v)
	}
	ra.Release(0)
}

func TestRCUArray_SwapCanReturnEarlyWithOutstandingReader(t *testing.T) {
	ra := NewRCUArrayFromSlice([]int{0})
	currentValue := 0
	deadline := time.Now().Add(5 * time.Second)

	for attempt := 1; time.Now().Before(deadline); attempt++ {
		releaseOld := make(chan struct{})
		oldReleased := make(chan struct{})
		acquired := make(chan struct{})

		go func() {
			_ = ra.Acquire(0)
			close(acquired)
			<-releaseOld
			ra.Release(0)
			close(oldReleased)
		}()

		select {
		case <-acquired:
		case <-time.After(time.Second):
			t.Fatalf("pre-swap acquire did not start in time")
		}

		swapDone := make(chan int, 1)
		go func(newVal int) {
			swapDone <- ra.Swap(0, newVal)
		}(attempt)

		// Encourage the writer to run and allow post-swap readers to race.
		runtime.Gosched()
		time.Sleep(time.Microsecond)

		postStop := make(chan struct{})
		postDone := make(chan struct{})
		go func() {
			defer close(postDone)
			for {
				select {
				case <-postStop:
					return
				default:
				}
				_ = ra.Acquire(0)
				ra.Release(0)
				runtime.Gosched()
			}
		}()

		timer := time.NewTimer(50 * time.Millisecond)
		var (
			returned bool
			prev     int
		)

		select {
		case prev = <-swapDone:
			returned = true
		case <-timer.C:
		}
		if !returned && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		if returned {
			close(releaseOld)
			<-oldReleased
			close(postStop)
			<-postDone
			if prev != currentValue {
				t.Fatalf("Swap returned %d, expected %d", prev, currentValue)
			}
			t.Fatalf("Swap returned while pre-swap acquire remained outstanding (attempt %d)", attempt)
		}

		close(releaseOld)
		<-oldReleased
		prev = <-swapDone
		close(postStop)
		<-postDone

		if prev != currentValue {
			t.Fatalf("Swap returned %d, expected %d", prev, currentValue)
		}
		currentValue = attempt
	}
}

func TestRCUArray_HighContention(t *testing.T) {
	const (
		nIdx     = 8
		nRead    = 8
		nWrite   = 4
		duration = 200 * time.Millisecond
	)
	ra := NewRCUArray[int](nIdx)

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Writers continuously swap values.
	for w := 0; w < nWrite; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			idx := id % nIdx
			val := id
			for !stop.Load() {
				_ = ra.Swap(idx, val)
				// Yield to allow readers to run.
				runtime.Gosched()
			}
		}(w)
	}

	// Readers continuously acquire/release ensuring they do not block.
	errCh := make(chan error, nRead)
	for r := 0; r < nRead; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			idx := id % nIdx
			deadline := time.Now().Add(duration)
			for time.Now().Before(deadline) {
				start := time.Now()
				_ = ra.Acquire(idx)
				ra.Release(idx)
				if time.Since(start) > 50*time.Millisecond {
					errCh <- fmt.Errorf("Acquire/Release blocked for %v", time.Since(start))
					return
				}
			}
			errCh <- nil
		}(r)
	}

	time.Sleep(duration)
	stop.Store(true)
	wg.Wait()

	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestRCUArray_GetContention(t *testing.T) {
	const (
		nIdx     = 4
		nRead    = 8
		nWrite   = 2
		duration = 150 * time.Millisecond
	)
	ra := NewRCUArray[int](nIdx)

	var stop atomic.Bool
	var wg sync.WaitGroup

	for w := 0; w < nWrite; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			idx := id % nIdx
			val := id
			for !stop.Load() {
				_ = ra.Swap(idx, val)
				runtime.Gosched()
			}
		}(w)
	}

	errCh := make(chan error, nRead)
	for r := 0; r < nRead; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			idx := id % nIdx
			deadline := time.Now().Add(duration)
			for time.Now().Before(deadline) {
				start := time.Now()
				_ = ra.Get(idx)
				if time.Since(start) > 50*time.Millisecond {
					errCh <- fmt.Errorf("Get blocked for %v", time.Since(start))
					return
				}
			}
			errCh <- nil
		}(r)
	}

	time.Sleep(duration)
	stop.Store(true)
	wg.Wait()

	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}
