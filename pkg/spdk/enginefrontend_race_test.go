package spdk

import (
	"fmt"
	"sync"

	. "gopkg.in/check.v1"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestEngineFrontendResumeConcurrentWithValidateAndUpdate(c *C) {
	fmt.Println("Testing EngineFrontend.Resume concurrent with ValidateAndUpdate")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1024, 0, 0, testIPFamily(), make(chan interface{}, 4096), nil)
	ef.State = lhtypes.InstanceStateRunning

	const iterations = 200

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if err := ef.Resume(nil); err != nil {
				errCh <- err
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if err := ef.ValidateAndUpdate(nil); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		c.Assert(err, IsNil)
	}

	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateRunning))
	c.Assert(ef.ErrorMsg, Equals, "")
}
