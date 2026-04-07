package spdk

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestEngineFrontendCreateReturnsErrorForFrontendFailure(c *C) {
	fmt.Println("Testing EngineFrontend.Create returns an error while preserving the error state")

	ef := NewEngineFrontend("ef-test", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend = nil

	resp, err := ef.Create(nil, "10.0.0.1:9502")
	c.Assert(err, NotNil)
	c.Assert(resp, IsNil)

	got := ef.Get()
	c.Assert(got.State, Equals, string(lhtypes.InstanceStateError))
	c.Assert(got.ErrorMsg, Matches, ".*invalid NvmeTcpFrontend.*")
}

// TestCreateDoesNotHoldLockWhileSendingUpdate verifies that Create releases
// the struct lock before blocking on UpdateCh, so concurrent Get() calls
// are not starved.
func (s *TestSuite) TestCreateDoesNotHoldLockWhileSendingUpdate(c *C) {
	fmt.Println("Testing Create does not hold the lock while sending update")

	// Unbuffered channel: Create will block on the send after setting state.
	ef := NewEngineFrontend("ef-create-lock", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1024, 0, 0, make(chan interface{}))

	errCh := make(chan error, 1)
	go func() {
		_, err := ef.Create(nil, "127.0.0.1:9500")
		errCh <- err
	}()

	// Wait for Create to reach Running (state is visible after Unlock,
	// before the blocking UpdateCh send).
	deadline := time.Now().Add(2 * time.Second)
	for ef.Get().State != string(lhtypes.InstanceStateRunning) {
		if time.Now().After(deadline) {
			c.Fatal("timeout waiting for engine frontend to enter running state")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Get() must not block — proves the lock was released before the
	// channel send.
	getDone := make(chan struct{}, 1)
	go func() {
		_ = ef.Get()
		getDone <- struct{}{}
	}()

	select {
	case <-getDone:
		// expected
	case <-time.After(1 * time.Second):
		c.Fatal("Get() blocked while Create is waiting on UpdateCh; lock may still be held")
	}

	// Unblock Create by draining the update channel.
	select {
	case <-ef.UpdateCh:
	case <-time.After(1 * time.Second):
		c.Fatal("timeout waiting for Create() update signal")
	}

	select {
	case err := <-errCh:
		c.Assert(err, IsNil)
	case <-time.After(1 * time.Second):
		c.Fatal("timeout waiting for Create() to return")
	}
}

// TestConcurrentCreateHasSingleWinner verifies that only one concurrent
// Create call on the same EngineFrontend succeeds; all others receive a
// precondition error.
func (s *TestSuite) TestConcurrentCreateHasSingleWinner(c *C) {
	fmt.Println("Testing concurrent Create has a single winner")

	ef := NewEngineFrontend("ef-create-concurrent", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1024, 0, 0, make(chan interface{}, 32))

	const workers = 20
	startCh := make(chan struct{})
	var wg sync.WaitGroup
	var successCount int32
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			_, err := ef.Create(nil, "127.0.0.1:9500")
			if err == nil {
				atomic.AddInt32(&successCount, 1)
				return
			}
			errCh <- err
		}()
	}

	close(startCh)
	wg.Wait()
	close(errCh)

	c.Assert(successCount, Equals, int32(1))
	c.Assert(len(errCh), Equals, workers-1)

	for err := range errCh {
		c.Assert(err, NotNil)
		errStr := err.Error()
		c.Assert(strings.Contains(errStr, "already creating") || strings.Contains(errStr, "invalid state"), Equals, true,
			Commentf("unexpected concurrent create error: %v", err))
	}

	c.Assert(ef.Get().State, Equals, string(lhtypes.InstanceStateRunning))
}
