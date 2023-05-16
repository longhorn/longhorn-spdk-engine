package types

import "fmt"

const (
	DefaultJSONServerNetwork    = "unix"
	DefaultUnixDomainSocketPath = "/var/tmp/spdk.sock"

	LocalIP = "127.0.0.1"

	MiB = 1 << 20

	FrontendSPDKTCPNvmf     = "spdk-tcp-nvmf"
	FrontendSPDKTCPBlockdev = "spdk-tcp-blockdev"
)

func GetNQN(name string) string {
	return fmt.Sprintf("nqn.2023-01.io.longhorn.spdk:%s", name)
}
