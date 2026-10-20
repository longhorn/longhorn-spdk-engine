package spdk

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	. "gopkg.in/check.v1"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestNvmeTCPPathIsOptimized(c *C) {
	const snapshot = `{"Subsystems":[{"NQN":"nqn.test","Paths":[{"Name":"nvme1","Transport":"tcp","Address":"traddr=10.0.0.2,trsvcid=3000","State":"live","ANAState":"optimized"}]}]}`
	for _, tc := range []struct {
		name   string
		output string
		ready  bool
	}{
		{"object format", snapshot, true},
		{"host array format", "[" + snapshot + "]", true},
		{"leading whitespace", "\n  " + snapshot, true},
		{"connecting", strings.Replace(snapshot, `"live"`, `"connecting"`, 1), false},
		{"deleting", strings.Replace(snapshot, `"live"`, `"deleting"`, 1), false},
		{"inaccessible", strings.Replace(snapshot, `"optimized"`, `"inaccessible"`, 1), false},
		{"non-optimized", strings.Replace(snapshot, `"optimized"`, `"non-optimized"`, 1), false},
		{"ANA transition", strings.Replace(snapshot, `"optimized"`, `"change"`, 1), false},
		{"missing ANA state", strings.Replace(snapshot, `,"ANAState":"optimized"`, "", 1), false},
		{"unknown ANA state", strings.Replace(snapshot, `"optimized"`, `"unknown"`, 1), false},
		{"missing controller state", strings.Replace(snapshot, `"State":"live",`, "", 1), false},
		{"wrong NQN", strings.Replace(snapshot, `nqn.test`, `nqn.other`, 1), false},
		{"wrong IP", strings.Replace(snapshot, `10.0.0.2`, `10.0.0.1`, 1), false},
		{"wrong port", strings.Replace(snapshot, `3000`, `2000`, 1), false},
		{"wrong transport", strings.Replace(snapshot, `"tcp"`, `"rdma"`, 1), false},
		{"missing controller name", strings.Replace(snapshot, `"nvme1"`, `""`, 1), false},
		{"no matching namespace paths", `[{"Subsystems":[]}]`, false},
		{"unknown object format", `{}`, false},
	} {
		ready, err := nvmeTCPPathIsOptimized(tc.output, "nqn.test", "10.0.0.2", "3000")
		c.Assert(err, IsNil, Commentf(tc.name))
		c.Assert(ready, Equals, tc.ready, Commentf(tc.name))
	}

	// Address matching must use the same normalization as targeted disconnect.
	ready, err := nvmeTCPPathIsOptimized(snapshot, "nqn.test", "::ffff:10.0.0.2", "3000")
	c.Assert(err, IsNil)
	c.Assert(ready, Equals, true)

	for _, output := range []string{"", "not JSON", "[", `{"Subsystems":"invalid"}`} {
		ready, err := nvmeTCPPathIsOptimized(output, "nqn.test", "10.0.0.2", "3000")
		c.Assert(err, NotNil)
		c.Assert(ready, Equals, false)
	}
	ready, err = nvmeTCPPathIsOptimized(snapshot, "", "10.0.0.2", "3000")
	c.Assert(err, NotNil)
	c.Assert(ready, Equals, false)
}

func (s *TestSuite) TestNvmeTCPPathReadinessRequiresNamespace(c *C) {
	ef := newBlockdevSwitchoverFrontend(c, make(chan interface{}, 1))
	ef.waitForNvmeTCPPathOptimizedFn = nil
	ef.initiator = nil
	c.Assert(ef.waitForNvmeTCPPathOptimized("nqn.test", "10.0.0.2", "3000"), NotNil)
	ef.initiator = &initiator.Initiator{NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: "nqn.test"}}
	c.Assert(ef.waitForNvmeTCPPathOptimized("nqn.test", "10.0.0.2", "3000"), NotNil)
}

func blockdevCleanupTestPhases(phased bool) []string {
	if phased {
		return []string{string(SwitchoverPhasePreparing), string(SwitchoverPhaseSwitching), string(SwitchoverPhasePromoting)}
	}
	return []string{""}
}

func (s *TestSuite) TestEngineFrontendCleanupFollowsPersistedHostReadyReplacement(c *C) {
	for _, phased := range []bool{false, true} {
		ef := newBlockdevSwitchoverFrontend(c, make(chan interface{}, 4))
		ef.metadataDir = c.MkDir()
		endpointLoaded := false
		hostReady := false
		disconnects := 0
		ef.loadInitiatorEndpointFn = func(bool) error {
			endpointLoaded = true
			return nil
		}
		ef.waitForNvmeTCPPathOptimizedFn = func(nqn, address, port string) error {
			c.Assert(endpointLoaded, Equals, true)
			c.Assert(nqn, Equals, getStableVolumeNQN("vol-a"))
			c.Assert(address, Equals, "10.0.0.2")
			c.Assert(port, Equals, "3000")
			c.Assert(ef.EngineName, Equals, "engine-b")
			records, err := loadEngineFrontendRecords(ef.metadataDir)
			c.Assert(err, IsNil)
			c.Assert(records, HasLen, 1)
			c.Assert(records[0].EngineName, Equals, "engine-b")
			c.Assert(records[0].TargetIP, Equals, "10.0.0.2")
			c.Assert(records[0].TargetPort, Equals, int32(3000))
			hostReady = true
			return nil
		}
		ef.disconnectStaleNvmeTCPPathFn = func(nqn, address, port string) error {
			c.Assert(hostReady, Equals, true)
			c.Assert(nqn, Equals, getStableVolumeNQN("vol-a"))
			c.Assert(address, Equals, "10.0.0.1")
			c.Assert(port, Equals, "2000")
			disconnects++
			return nil
		}

		phases := blockdevCleanupTestPhases(phased)
		for i, phase := range phases {
			c.Assert(ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", phase), IsNil, Commentf("phased=%v phase=%s", phased, phase))
			if i < len(phases)-1 {
				c.Assert(disconnects, Equals, 0)
				c.Assert(hostReady, Equals, false)
			}
		}
		c.Assert(disconnects, Equals, 1)
		c.Assert(string(ef.State), Equals, lhtypes.InstanceStateRunning)
	}
}

func (s *TestSuite) TestEngineFrontendCleanupSkipsAfterPersistenceFailure(c *C) {
	assertEngineFrontendCleanupSkipsUncertainReplacement(c, "persistence")
}

func (s *TestSuite) TestEngineFrontendCleanupSkipsUnoptimizedReplacement(c *C) {
	assertEngineFrontendCleanupSkipsUncertainReplacement(c, "host readiness")
}

func assertEngineFrontendCleanupSkipsUncertainReplacement(c *C, failure string) {
	for _, phased := range []bool{false, true} {
		ef := newBlockdevSwitchoverFrontend(c, make(chan interface{}, 4))
		ef.metadataDir = c.MkDir()
		if failure == "persistence" {
			// A file where a directory is required deterministically makes
			// persistence fail, including when tests are run as root.
			ef.metadataDir = filepath.Join(ef.metadataDir, "not-a-directory")
			c.Assert(os.WriteFile(ef.metadataDir, nil, 0600), IsNil)
		}
		readinessChecks := 0
		disconnects := 0
		ef.waitForNvmeTCPPathOptimizedFn = func(string, string, string) error {
			readinessChecks++
			return fmt.Errorf("replacement ANA state is not optimized")
		}
		ef.disconnectStaleNvmeTCPPathFn = func(string, string, string) error {
			disconnects++
			return nil
		}
		for _, phase := range blockdevCleanupTestPhases(phased) {
			c.Assert(ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", phase), IsNil,
				Commentf("phased=%v failure=%s phase=%s", phased, failure, phase))
		}
		c.Assert(disconnects, Equals, 0)
		if failure == "persistence" {
			c.Assert(readinessChecks, Equals, 0)
		} else {
			c.Assert(readinessChecks, Equals, 1)
		}
		c.Assert(ef.EngineName, Equals, "engine-b")
		c.Assert(string(ef.State), Equals, lhtypes.InstanceStateRunning)
		c.Assert(ef.ErrorMsg, Equals, "")
	}
}

func (s *TestSuite) TestEngineFrontendCleanupNeverRunsAfterFailedSwitchover(c *C) {
	for _, phased := range []bool{false, true} {
		for _, failure := range []string{"connect", "controller live", "ANA", "device reload", "endpoint reload"} {
			ef := newBlockdevSwitchoverFrontend(c, make(chan interface{}, 4))
			switch failure {
			case "connect":
				ef.connectNvmeTCPPathFn = func(string, string) error { return fmt.Errorf("connect failed") }
			case "controller live":
				ef.waitForNvmeTCPControllerLiveFn = func(string, int32) error { return fmt.Errorf("controller is not live") }
			case "ANA":
				ef.setRemoteEngineTargetANAStateFn = func(string, string, NvmeTCPANAState) error { return fmt.Errorf("ANA update failed") }
			case "device reload":
				ef.loadInitiatorNVMeDeviceInfoFn = func(string, string, string) error { return fmt.Errorf("device reload failed") }
			case "endpoint reload":
				ef.loadInitiatorEndpointFn = func(bool) error { return fmt.Errorf("endpoint reload failed") }
			}
			disconnects := 0
			readinessChecks := 0
			ef.disconnectStaleNvmeTCPPathFn = func(string, string, string) error {
				disconnects++
				return nil
			}
			ef.waitForNvmeTCPPathOptimizedFn = func(string, string, string) error {
				readinessChecks++
				return nil
			}
			var err error
			for _, phase := range blockdevCleanupTestPhases(phased) {
				err = ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", phase)
				if err != nil {
					break
				}
			}
			c.Assert(err, NotNil, Commentf("phased=%v failure=%s", phased, failure))
			c.Assert(disconnects, Equals, 0)
			c.Assert(readinessChecks, Equals, 0)
			c.Assert(ef.EngineName, Equals, "engine-a")
			c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.1")
		}
	}
}

func (s *TestSuite) TestDropSupersededNvmeTCPPathPreservesEquivalentAddress(c *C) {
	ef := newBlockdevSwitchoverFrontend(c, make(chan interface{}, 1))
	disconnects := 0
	readinessChecks := 0
	ef.disconnectStaleNvmeTCPPathFn = func(string, string, string) error {
		disconnects++
		return nil
	}
	ef.waitForNvmeTCPPathOptimizedFn = func(string, string, string) error {
		readinessChecks++
		return nil
	}
	nqn := getStableVolumeNQN("vol-a")
	ef.dropSupersededNvmeTCPPath(nqn, "10.0.0.1", 2000, "::ffff:10.0.0.1", 2000)
	ef.dropSupersededNvmeTCPPath(nqn, "2001:db8::1", 2000, "2001:0db8:0:0:0:0:0:1", 2000)
	ef.dropSupersededNvmeTCPPath(nqn, "10.0.0.1", -1, "10.0.0.2", 3000)
	ef.dropSupersededNvmeTCPPath(nqn, "10.0.0.1", 2000, "10.0.0.2", 65536)
	c.Assert(disconnects, Equals, 0)
	c.Assert(readinessChecks, Equals, 0)
}
