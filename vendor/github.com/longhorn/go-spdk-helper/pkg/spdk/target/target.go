package target

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	"github.com/longhorn/go-spdk-helper/pkg/types"
)

const (
	SPDKScriptsDir  = "scripts"
	SPDKSetupScript = "setup.sh"
	SPDKTGTBinary   = "build/bin/spdk_tgt"
)

// SetupTarget setups the spdk_tgt with the given args
func SetupTarget(spdkDir string, setupArgs []string, execute func(name string, args []string, timeout time.Duration) (string, error)) (err error) {
	setupArgsInStr := ""
	for _, arg := range setupArgs {
		setupArgsInStr = fmt.Sprintf("%s %s", setupArgsInStr, arg)
	}
	setupScriptPath := filepath.Join(spdkDir, SPDKScriptsDir, SPDKSetupScript)
	setupOpts := []string{
		"-c",
		fmt.Sprintf("%s %s", setupScriptPath, setupArgsInStr),
	}

	resetOpts := []string{
		"-c",
		setupScriptPath,
		"reset",
	}

	if _, err := execute("sh", resetOpts, types.ExecuteTimeout); err != nil {
		return err
	}
	if _, err := execute("sh", setupOpts, types.ExecuteTimeout); err != nil {
		return err
	}

	return nil
}

// StartTarget starts the spdk_tgt with the given args
func StartTarget(spdkDir string, args []string, execute func(binary string, args []string, timeout time.Duration) (string, error)) (err error) {
	if spdkCli, err := client.NewClient(context.Background()); err == nil {
		if _, err := spdkCli.BdevGetBdevs("", 0); err == nil {
			logrus.Info("Detected running spdk_tgt, skipped the target starting")
			return nil
		}
	}

	argsInStr := ""
	for _, arg := range args {
		argsInStr = fmt.Sprintf("%s %s", argsInStr, arg)
	}
	tgtOpts := []string{
		"-c",
		fmt.Sprintf("%s %s", filepath.Join(spdkDir, SPDKTGTBinary), argsInStr),
	}

	_, err = execute("sh", tgtOpts, types.ExecuteTimeout)
	return err
}
