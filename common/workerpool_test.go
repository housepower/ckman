package common

import (
	"fmt"
	"testing"
)

func TestWorkerPool(t *testing.T) {
	wp := NewWorkerPool(3, 1)
	defer wp.Close()
	for i := 0; i < 10000; i++ {
		wp.Restart()
		requests := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
		rspChan := make(chan string, len(requests))
		for _, r := range requests {
			r := r
			_ = wp.Submit(func() {
				rspChan <- r
			})
		}
		wp.StopWait()

		close(rspChan)
		rspSet := map[string]struct{}{}
		for rsp := range rspChan {
			rspSet[rsp] = struct{}{}
		}
		if len(rspSet) < len(requests) {
			t.Fatal("Did not handle all requests")
		}
		for _, req := range requests {
			if _, ok := rspSet[req]; !ok {
				t.Fatal("Missing expected values:", req)
			}
		}
	}
}

func TestPool(t *testing.T) {
	wp := NewWorkerPool(3, 1)
	defer wp.Close()
	wp.Resize(10)
	for i := 0; i < 10000; i++ {
		wp.Restart()
		requests := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
		rspChan := make(chan string, len(requests))
		for _, r := range requests {
			r := r
			_ = wp.Submit(func() {
				rspChan <- r
			})
		}
		fmt.Println("pending = ", wp.Pending())
		wp.Wait()

		close(rspChan)
		rspSet := map[string]struct{}{}
		for rsp := range rspChan {
			rspSet[rsp] = struct{}{}
		}
		if len(rspSet) < len(requests) {
			t.Fatal("Did not handle all requests")
		}
		for _, req := range requests {
			if _, ok := rspSet[req]; !ok {
				t.Fatal("Missing expected values:", req)
			}
		}
	}
}
