package types

import "fmt"

const (
	DefaultJSONServerNetwork    = "unix"
	DefaultUnixDomainSocketPath = "/var/tmp/spdk.sock"

	LocalIP = "127.0.0.1"

	MiB = 1 << 20

	FrontendSPDKTCPNvmf     = "spdk-tcp-nvmf"
	FrontendSPDKTCPBlockdev = "spdk-tcp-blockdev"

	DefaultCtrlrLossTimeoutSec = 30
	// ReconnectDelaySec can't be more than FastIoFailTimeoutSec
	DefaultReconnectDelaySec    = 5
	DefaultFastIoFailTimeoutSec = 15
)

func GetNQN(name string) string {
	return fmt.Sprintf("nqn.2023-01.io.longhorn.spdk:%s", name)
}
