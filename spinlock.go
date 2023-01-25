package atur

import (
	"runtime"
	"sync/atomic"
)

type SpinLock struct {
	lock uintptr
}

func (l *SpinLock) Lock() {
loop:
	if !atomic.CompareAndSwapUintptr(&l.lock, 0, 1) {
		runtime.Gosched()
		goto loop
	}
}

func (l *SpinLock) Unlock() {
	atomic.StoreUintptr(&l.lock, 0)
}
