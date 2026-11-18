package spdk

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"

	commontypes "github.com/longhorn/go-common-libs/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
)

// waitForNvmeTCPPathOptimized observes the host's view of the replacement path.
// A successful target-side ANA update does not mean the kernel has processed
// its notification yet. If readiness cannot be confirmed, callers retain the
// old controller and let ctrl-loss-tmo handle cleanup.
func (ef *EngineFrontend) waitForNvmeTCPPathOptimized(nqn, transportAddress, transportServiceID string) error {
	if ef.waitForNvmeTCPPathOptimizedFn != nil {
		return ef.waitForNvmeTCPPathOptimizedFn(nqn, transportAddress, transportServiceID)
	}
	if ef.initiator == nil {
		return fmt.Errorf("cannot confirm replacement path without an initiator")
	}
	namespace := ef.initiator.GetNamespaceName()
	if namespace == "" || strings.Contains(namespace, "/") {
		return fmt.Errorf("cannot confirm replacement path without a namespace name")
	}

	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return fmt.Errorf("create executor for NVMe-TCP path readiness: %w", err)
	}

	var lastErr error
	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true,
		func(ctx context.Context) (bool, error) {
			timeout := time.Second
			if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < timeout {
				timeout = time.Until(deadline)
			}
			if timeout <= 0 {
				return false, nil
			}
			// nvme-cli only includes namespace-specific ANAState when a
			// namespace device is supplied to list-subsys.
			output, err := executor.Execute(nil, "nvme", []string{"list-subsys", "-o", "json", "/dev/" + namespace}, timeout)
			if err != nil {
				lastErr = err
				return false, nil
			}
			ready, err := nvmeTCPPathIsOptimized(output, nqn, transportAddress, transportServiceID)
			lastErr = err
			return ready, nil
		})
	if err != nil {
		return fmt.Errorf("NVMe-TCP path %s:%s for %s is not confirmed live and ANA-optimized: %w (last probe error: %v)",
			transportAddress, transportServiceID, nqn, err, lastErr)
	}
	return nil
}

// nvmeTCPPathIsOptimized accepts the nvme-cli 1.x object and 2.x host-array
// formats. list-subsys reads ANAState from the host's namespace paths, unlike
// ana-log, which would only confirm the target's state. Longhorn frontends
// expose one namespace per volume-scoped NQN. Missing state fails closed.
func nvmeTCPPathIsOptimized(output, nqn, transportAddress, transportServiceID string) (bool, error) {
	type path struct {
		Name      string `json:"Name"`
		Transport string `json:"Transport"`
		Address   string `json:"Address"`
		State     string `json:"State"`
		ANAState  string `json:"ANAState"`
	}
	type subsystem struct {
		NQN   string `json:"NQN"`
		Paths []path `json:"Paths"`
	}
	type host struct {
		Subsystems []subsystem `json:"Subsystems"`
	}

	if nqn == "" || transportAddress == "" || transportServiceID == "" {
		return false, fmt.Errorf("incomplete replacement NVMe-TCP path identity")
	}

	var hosts []host
	if strings.HasPrefix(strings.TrimSpace(output), "{") {
		var singleHost host
		if err := json.Unmarshal([]byte(output), &singleHost); err != nil {
			return false, fmt.Errorf("decode NVMe subsystems: %w", err)
		}
		hosts = append(hosts, singleHost)
	} else if err := json.Unmarshal([]byte(output), &hosts); err != nil {
		return false, fmt.Errorf("decode NVMe subsystems: %w", err)
	}
	for _, h := range hosts {
		for _, s := range h.Subsystems {
			if s.NQN != nqn {
				continue
			}
			for _, p := range s.Paths {
				ip, port := initiator.GetIPAndPortFromControllerAddress(p.Address)
				if p.Name != "" && p.Transport == "tcp" &&
					helperutil.IsSameNvmeAddr(ip, transportAddress) && port == transportServiceID &&
					p.State == initiator.NvmeControllerStateLive && p.ANAState == string(NvmeTCPANAStateOptimized) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}
