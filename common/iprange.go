package common

import (
	"math"
	"net"
	"strings"

	"github.com/pkg/errors"
)

func ParseHosts(hosts []string) ([]string, error) {
	var allHosts []string
	for _, host := range hosts {
		ips, err := ParseIPRange(host)
		if err != nil {
			return allHosts, err
		}
		allHosts = append(allHosts, ips...)
	}
	return allHosts, nil
}

func ParseIPRange(s string) ([]string, error) {
	var ips []string
	var err error
	if strings.Contains(s, "-") {
		if ips, err = ipRangeParse(s); err != nil {
			return ips, err
		}
	} else if strings.Contains(s, "/") {
		ips, err = ipCIDR(s)
		if err != nil {
			return ips, err
		}
	} else {
		ips = append(ips, s)
	}

	return ips, nil
}

//TODO how about ipv6?
//https://github.com/thinkeridea/go-extend/blob/main/exnet/ip.go
func InetAtoN(ip string) (uint, error) {
	b := net.ParseIP(ip).To4()
	if b == nil {
		return 0, errors.New("invalid ipv4 format")
	}

	return uint(b[3]) | uint(b[2])<<8 | uint(b[1])<<16 | uint(b[0])<<24, nil
}

func InetNtoA(i uint) (string, error) {
	if i > math.MaxUint32 {
		return "", errors.New("beyond the scope of ipv4")
	}

	ip := make(net.IP, net.IPv4len)
	ip[0] = byte(i >> 24)
	ip[1] = byte(i >> 16)
	ip[2] = byte(i >> 8)
	ip[3] = byte(i)

	return ip.String(), nil
}

func ipRangeParse(s string) ([]string, error) {
	var ips []string
	ipAddrs := strings.Split(s, "-")
	if len(ipAddrs) != 2 {
		return ips, errors.Errorf("invaild ip range")
	}
	begin, err := InetAtoN(ipAddrs[0])
	if err != nil {
		return ips, err
	}
	end, err := InetAtoN(ipAddrs[1])
	if err != nil {
		return ips, err
	}

	if begin > end {
		return ips, errors.Errorf("invalid ip range: %s", s)
	}

	for num := begin; num <= end; num++ {
		if ip, err := InetNtoA(num); err != nil {
			return ips, err
		} else {
			ips = append(ips, ip)
		}
	}
	return ips, nil
}

func ipCIDR(s string) ([]string, error) {
	ip, ipnet, err := net.ParseCIDR(s)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	var ips []string
	for ip := ip.Mask(ipnet.Mask); ipnet.Contains(ip); inc(ip) {
		ips = append(ips, ip.String())
	}
	return ips, nil
}

func inc(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
}
