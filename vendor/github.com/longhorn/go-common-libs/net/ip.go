package net

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
)

const (
	EnvPodIP = "POD_IP"

	StorageNetworkInterface = "lhnet1"
)

// IPFamily identifies an IP address family.
type IPFamily string

const (
	IPFamilyUnspecified IPFamily = ""
	IPFamilyIPv4        IPFamily = "ipv4"
	IPFamilyIPv6        IPFamily = "ipv6"
)

// ParseIPFamily parses an IP family case-insensitively.
func ParseIPFamily(value string) (IPFamily, error) {
	family := IPFamily(strings.ToLower(value))
	switch family {
	case IPFamilyUnspecified, IPFamilyIPv4, IPFamilyIPv6:
		return family, nil
	default:
		return "", fmt.Errorf("invalid IP family %q", value)
	}
}

// getInterfaceAddrs returns the addresses for an interface. A false found
// value means that the interface does not exist.
func getInterfaceAddrs(name string) (addrs []net.Addr, found bool, err error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, true, err
	}

	var iface *net.Interface
	for i := range interfaces {
		if interfaces[i].Name == name {
			iface = &interfaces[i]
			break
		}
	}
	if iface == nil {
		return nil, false, nil
	}
	addrs, err = iface.Addrs()
	if err != nil {
		return nil, true, errors.Wrapf(err, "interface %s doesn't have address", name)
	}

	return addrs, true, nil
}

func getIPFromAddr(addr net.Addr) net.IP {
	switch addr := addr.(type) {
	case *net.IPNet:
		if addr == nil {
			return nil
		}
		return addr.IP
	case *net.IPAddr:
		if addr == nil {
			return nil
		}
		return addr.IP
	default:
		return nil
	}
}

func getLocalIPFromAddrsByFamily(addrs []net.Addr, family IPFamily) string {
	for _, addr := range addrs {
		ip := getIPFromAddr(addr)
		if ip == nil {
			continue
		}

		switch family {
		case IPFamilyIPv4:
			if ipv4 := ip.To4(); ipv4 != nil {
				return ipv4.String()
			}
		case IPFamilyIPv6:
			if ip.To4() == nil {
				if ipv6 := ip.To16(); ipv6 != nil && ipv6.IsGlobalUnicast() {
					return ipv6.String()
				}
			}
		}
	}

	return ""
}

func getInterfaceNameByIP(ip net.IP) (string, error) {
	if ip == nil {
		return "", nil
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			return "", errors.Wrapf(err, "interface %s doesn't have address", iface.Name)
		}

		for _, addr := range addrs {
			addrIP := getIPFromAddr(addr)
			if addrIP != nil && addrIP.To16() != nil && addrIP.Equal(ip) {
				return iface.Name, nil
			}
		}
	}

	return "", nil
}

// GetLocalIPv4fromInterface returns the local IPv4 address.
func GetLocalIPv4fromInterface(name string) (ip string, err error) {
	addrs, found, err := getInterfaceAddrs(name)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("interface %s doesn't exist", name)
	}

	if ip := getLocalIPFromAddrsByFamily(addrs, IPFamilyIPv4); ip != "" {
		return ip, nil
	}

	return "", errors.Errorf("interface %s don't have an IPv4 address", name)
}

func getIPForPod(family IPFamily, podIP string,
	interfaceAddrs func(string) ([]net.Addr, bool, error),
	interfaceNameByIP func(net.IP) (string, error)) (string, error) {
	switch family {
	case IPFamilyUnspecified:
		addrs, found, err := interfaceAddrs(StorageNetworkInterface)
		if found && err == nil {
			if ip := getLocalIPFromAddrsByFamily(addrs, IPFamilyIPv4); ip != "" {
				return ip, nil
			}
		}
		if podIP != "" {
			return podIP, nil
		}
		return "", fmt.Errorf("can't get a ip from either the specified interface or the environment variable")
	case IPFamilyIPv4, IPFamilyIPv6:
	default:
		return "", fmt.Errorf("invalid IP family %q", family)
	}

	addrs, found, err := interfaceAddrs(StorageNetworkInterface)
	if found {
		if err != nil {
			return "", err
		}
		if ip := getLocalIPFromAddrsByFamily(addrs, family); ip != "" {
			return ip, nil
		}
		return "", fmt.Errorf("storage network interface %s has no %s address", StorageNetworkInterface, family)
	}

	parsedPodIP := net.ParseIP(podIP)
	if ipMatchesFamily(parsedPodIP, family) {
		return podIP, nil
	}

	if parsedPodIP != nil {
		interfaceName, err := interfaceNameByIP(parsedPodIP)
		if err != nil {
			return "", err
		}
		if interfaceName != "" {
			addrs, found, err := interfaceAddrs(interfaceName)
			if err != nil {
				return "", err
			}
			if found {
				if ip := getLocalIPFromAddrsByFamily(addrs, family); ip != "" {
					return ip, nil
				}
			}
		}
	}

	return "", fmt.Errorf("can't get a ip from either the specified interface or the environment variable")
}

func ipMatchesFamily(ip net.IP, family IPFamily) bool {
	if ip == nil {
		return false
	}

	switch family {
	case IPFamilyIPv4:
		return ip.To4() != nil
	case IPFamilyIPv6:
		if ip.To4() != nil {
			return false
		}
		ipv6 := ip.To16()
		return ipv6 != nil && ipv6.IsGlobalUnicast()
	default:
		return false
	}
}

// GetIPForPod returns the pod IP using the storage network IPv4 address when
// available, and otherwise falls back to the raw POD_IP value.
func GetIPForPod() (ip string, err error) {
	return GetIPForPodByFamily(IPFamilyUnspecified)
}

// GetIPForPodByFamily returns the pod IP for the requested address family.
func GetIPForPodByFamily(family IPFamily) (ip string, err error) {
	return getIPForPod(
		family,
		os.Getenv(EnvPodIP),
		getInterfaceAddrs,
		getInterfaceNameByIP,
	)
}

// IsLoopbackHost checks if the given host is a loopback host.
func IsLoopbackHost(host string) bool {
	if host == "localhost" || host == "127.0.0.1" || host == "0.0.0.0" || host == "::1" || host == "" {
		return true
	}
	// Check for loopback network.
	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}

	for _, ip := range ips {
		if !ip.IsLoopback() {
			return false
		}
	}

	return true
}

// GetAnyExternalIP returns any external IP address.
func GetAnyExternalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}

		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}

		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}

	return "", fmt.Errorf("the current host is probably not connected to the network")
}
