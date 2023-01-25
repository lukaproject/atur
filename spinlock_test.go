package atur_test

import (
	"sync"
	"testing"

	"github.com/lukaproject/atur"
)

func testLock(gorutines, n int, l sync.Locker) {
	var wg sync.WaitGroup
	wg.Add(gorutines)

	var count1 int
	var count2 int

	for i := 0; i < gorutines; i++ {
		go func() {
			for i := 0; i < n; i++ {
				l.Lock()
				count1++
				count2 += 2
				l.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if count1 != gorutines*n {
		panic("mismatch")
	}
	if count2 != gorutines*n*2 {
		panic("mismatch")
	}
}

func BenchmarkSpinLock_In_1Gorutine(b *testing.B) {
	testLock(1, 1000000, &atur.SpinLock{})
}

func BenchmarkSpinLock_In_4Gorutine(b *testing.B) {
	testLock(4, 1000000, &atur.SpinLock{})
}

func BenchmarkSpinLock_In_8Gorutine(b *testing.B) {
	testLock(8, 1000000, &atur.SpinLock{})
}
