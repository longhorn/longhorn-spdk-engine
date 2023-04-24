package nvme

import (
	"bufio"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/longhorn/go-spdk-helper/pkg/util"
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

type Controller struct {
	Controller   string
	Transport    string
	Address      string
	State        string
	HostNQN      string
	HostID       string
	Firmware     string
	ModelNumber  string
	SerialNumber string
	Namespaces   []Namespace
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
	}

	// A valid output is like below:
	//   Discovery Log Number of Records 1, Generation counter 1
	//   =====Discovery Log Entry 0======
	//   trtype:  tcp
	//   adrfam:  ipv4
	//   subtype: nvme subsystem
	//   treq:    not required
	//   portid:  0
	//   trsvcid: 4520
	//   subnqn:  nqn.2023-01.io.spdk:raid01
	//   traddr:  127.0.0.1
	//   sectype: none
	output, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "subnqn:") {
			continue
		}
		subnqn = strings.TrimSpace(strings.TrimPrefix(line, "subnqn:"))
		break
	}
	if subnqn == "" {
		return "", fmt.Errorf("found empty subnqn after nvme discover for %s:%s", ip, port)
	}

	return subnqn, nil
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

	output := map[string]string{}
	if err := json.Unmarshal([]byte(outputStr), &output); err != nil {
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

func GetDevices(ip, port, nqn string, executor util.Executor) (devices []Device, err error) {
	opts := []string{
		"list",
		"-v",
		"-o", "json",
	}

	// The output example:
	// {
	//  "Devices" : [
	//    {
	//      "Subsystem" : "nvme-subsys0",
	//      "SubsystemNQN" : "nqn.2023-01.io.longhorn.spdk:raid01",
	//      "Controllers" : [
	//        {
	//          "Controller" : "nvme0",
	//          "Transport" : "tcp",
	//          "Address" : "traddr=127.0.0.1 trsvcid=4520",
	//          "State" : "live",
	//          "HostNQN" : "nqn.2014-08.org.nvmexpress:uuid:f9851252-f382-4eb8-af24-a5fbd875157a",
	//          "HostID" : "d9bcbe5a-ecad-4dc7-bd65-babcc0f990bd",
	//          "Firmware" : "23.05",
	//          "ModelNumber" : "SPDK bdev Controller",
	//          "SerialNumber" : "00000000000000000000",
	//          "Namespaces" : [
	//            {
	//              "NameSpace" : "nvme0c0n1",
	//              "NSID" : 1,
	//              "UsedBytes" : 0,
	//              "MaximumLBA" : 0,
	//              "PhysicalSize" : 0,
	//              "SectorSize" : 1
	//            }
	//          ]
	//        }
	//      ],
	//      "Namespaces" : [
	//        {
	//          "NameSpace" : "nvme0n1",
	//          "NSID" : 1,
	//          "UsedBytes" : 4194304,
	//          "MaximumLBA" : 1024,
	//          "PhysicalSize" : 4194304,
	//          "SectorSize" : 4096
	//        }
	//      ]
	//    }
	//  ]
	// }
	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return nil, err
	}
	output := map[string][]Device{}
	if err := json.Unmarshal([]byte(outputStr), &output); err != nil {
		return nil, err
	}

	if nqn == "" {
		return output["Devices"], err
	}

	res := []Device{}
	for _, d := range output["Devices"] {
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
