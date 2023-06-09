package nvme

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/longhorn/go-spdk-helper/pkg/util"
	"github.com/sirupsen/logrus"
)

const (
	nvmeBinary = "nvme"

	DefaultTransportType = "tcp"
)

type Device struct {
	Subsystem    string
	SubsystemNQN string
	Controllers  []Controller
	Namespaces   []Namespace
}

type DiscoveryPageEntry struct {
	PortID  uint16 `json:"portid"`
	TrsvcID string `json:"trsvcid"`
	Subnqn  string `json:"subnqn"`
	Traddr  string `json:"traddr"`
	SubType string `json:"subtype"`
}

type Controller struct {
	Controller string
	Transport  string
	Address    string
	State      string
}

type Namespace struct {
	NameSpace    string
	NSID         uint32
	UsedBytes    uint64
	MaximumLBA   uint32
	PhysicalSize uint64
	SectorSize   uint32
}

func CheckForNVMeCliExistence(executor util.Executor) error {
	opts := []string{
		"--version",
	}
	_, err := executor.Execute(nvmeBinary, opts)
	return err
}

func DiscoverTarget(ip, port string, executor util.Executor) (subnqn string, err error) {
	opts := []string{
		"discover",
		"-t", DefaultTransportType,
		"-a", ip,
		"-s", port,
		"-o", "json",
	}

	// A valid output is like below:
	// # nvme discover -t tcp -a 10.42.2.20 -s 20011 -o json
	//	{
	//		"device" : "nvme0",
	//		"genctr" : 2,
	//		"records" : [
	//		  {
	//			"trtype" : "tcp",
	//			"adrfam" : "ipv4",
	//			"subtype" : "nvme subsystem",
	//			"treq" : "not required",
	//			"portid" : 0,
	//			"trsvcid" : "20001",
	//			"subnqn" : "nqn.2023-01.io.longhorn.spdk:pvc-81bab972-8e6b-48be-b691-18eaa430a897-r-0881c7b4",
	//			"traddr" : "10.42.2.20",
	//			"sectype" : "none"
	//		  },
	//		  {
	//			"trtype" : "tcp",
	//			"adrfam" : "ipv4",
	//			"subtype" : "nvme subsystem",
	//			"treq" : "not required",
	//			"portid" : 0,
	//			"trsvcid" : "20011",
	//			"subnqn" : "nqn.2023-01.io.longhorn.spdk:pvc-5f94d59d-baec-40e5-9e8b-25b79909d14e-e-49c947f5",
	//			"traddr" : "10.42.2.20",
	//			"sectype" : "none"
	//		  }
	//		]
	//	  }

	// nvme discover does not respect the -s option, so we need to filter the output
	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return "", err
	}

	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return "", err
	}

	var output struct {
		Entries []DiscoveryPageEntry `json:"records"`
	}

	err = json.Unmarshal([]byte(jsonStr), &output)
	if err != nil {
		return "", err
	}

	for _, entry := range output.Entries {
		if entry.TrsvcID == port {
			return entry.Subnqn, nil
		}
	}

	return "", fmt.Errorf("found empty subnqn after nvme discover for %s:%s", ip, port)
}

func ConnectTarget(ip, port, nqn string, executor util.Executor) (controllerName string, err error) {
	opts := []string{
		"connect",
		"-t", DefaultTransportType,
		"-a", ip,
		"-s", port,
		"--nqn", nqn,
		"-o", "json",
	}

	// Trying to connect an existing subsystem will error out with exit code 114.
	// Hence, it's better to check the existence first.
	if devices, err := GetDevices(ip, port, nqn, executor); err == nil && len(devices) > 0 {
		return devices[0].Controllers[0].Controller, nil
	}

	// The output example:
	// {
	//  "device" : "nvme0"
	// }
	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return "", err
	}

	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return "", err
	}

	output := map[string]string{}
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return "", err
	}

	return output["device"], nil
}

func DisconnectTarget(nqn string, executor util.Executor) error {
	opts := []string{
		"disconnect",
		"--nqn", nqn,
	}

	// The output example:
	// NQN:nqn.2023-01.io.spdk:raid01 disconnected 1 controller(s)
	//
	// And trying to disconnect a non-existing target would return exit code 0
	_, err := executor.Execute(nvmeBinary, opts)
	return err
}

type Subsystem struct {
	Name  string
	NQN   string
	Paths []Path
}

type Path struct {
	Name      string
	Transport string
	Address   string
	State     string
}

func listSubsystems(device string, executor util.Executor) ([]Subsystem, error) {
	opts := []string{
		"list-subsys",
		device,
		"-o", "json",
	}
	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return nil, err
	}
	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return nil, err
	}
	output := map[string][]Subsystem{}
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return nil, err
	}

	return output["Subsystems"], nil
}

type NvmeDevice struct {
	NameSpace    uint32
	DevicePath   string
	Firmware     string
	Index        uint32
	ModelNumber  string
	SerialNumber string
	UsedBytes    uint64
	MaximumLBA   uint32
	PhysicalSize uint64
	SectorSize   uint32
}

func listNvmeDevices(executor util.Executor) ([]NvmeDevice, error) {
	opts := []string{
		"list",
		"-o", "json",
	}
	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return nil, err
	}
	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return nil, err
	}
	output := map[string][]NvmeDevice{}
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return nil, err
	}

	return output["Devices"], nil
}

// GetDevices returns all devices
func GetDevices(ip, port, nqn string, executor util.Executor) (devices []Device, err error) {
	devices = []Device{}

	nvmeDevices, err := listNvmeDevices(executor)
	if err != nil {
		return nil, err
	}
	for _, d := range nvmeDevices {
		// Get subsystem
		subsystems, err := listSubsystems(d.DevicePath, executor)
		if err != nil {
			logrus.WithError(err).Warnf("failed to get subsystem for nvme device %s", d.DevicePath)
			continue
		}
		if len(subsystems) == 0 {
			return nil, fmt.Errorf("no subsystem found for nvme device %s", d.DevicePath)
		}
		if len(subsystems) > 1 {
			return nil, fmt.Errorf("multiple subsystems found for nvme device %s", d.DevicePath)
		}
		sys := subsystems[0]

		// Reconstruct controller list
		controllers := []Controller{}
		for _, path := range sys.Paths {
			controller := Controller{
				Controller: path.Name,
				Transport:  path.Transport,
				Address:    path.Address,
				State:      path.State,
			}
			controllers = append(controllers, controller)
		}

		namespace := Namespace{
			NameSpace:    filepath.Base(d.DevicePath),
			NSID:         d.NameSpace,
			UsedBytes:    d.UsedBytes,
			MaximumLBA:   d.MaximumLBA,
			PhysicalSize: d.PhysicalSize,
			SectorSize:   d.SectorSize,
		}

		device := Device{
			Subsystem:    sys.Name,
			SubsystemNQN: sys.NQN,
			Controllers:  controllers,
			Namespaces:   []Namespace{namespace},
		}

		devices = append(devices, device)
	}

	if nqn == "" {
		return devices, err
	}

	res := []Device{}
	for _, d := range devices {
		match := false
		if d.SubsystemNQN != nqn {
			continue
		}
		for _, c := range d.Controllers {
			controllerIP, controllerPort := GetIPAndPortFromControllerAddress(c.Address)
			if ip != "" && ip != controllerIP {
				continue
			}
			if port != "" && port != controllerPort {
				continue
			}
			match = true
			break
		}
		if len(d.Namespaces) == 0 {
			continue
		}
		if match {
			res = append(res, d)
		}
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("cannot find a valid nvme device with subsystem NQN %s and address %s:%s", nqn, ip, port)
	}
	return res, nil
}

func GetIPAndPortFromControllerAddress(addr string) (ip, port string) {
	reg := regexp.MustCompile(`traddr=([^"]*) trsvcid=\d*$`)
	ip = reg.ReplaceAllString(addr, "${1}")
	reg = regexp.MustCompile(`traddr=.* trsvcid=([^"]*)$`)
	port = reg.ReplaceAllString(addr, "${1}")
	return ip, port
}

// Extract the JSON part without the prefix
func extractJSONString(str string) (string, error) {
	startIndex := strings.Index(str, "{")
	if startIndex == -1 {
		return "", fmt.Errorf("invalid JSON string")
	}
	return str[startIndex:], nil
}
