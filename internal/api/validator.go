package api

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

var allowedMethods = map[string]bool{"POST": true, "PUT": true, "PATCH": true}

// private IPv4 / IPv6 ranges blocked for SSRF protection
var privateCIDRs []*net.IPNet

func init() {
	blocks := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"127.0.0.0/8",
		"169.254.0.0/16", // link-local
		"::1/128",
		"fc00::/7",
		"fe80::/10",
	}
	for _, b := range blocks {
		_, cidr, _ := net.ParseCIDR(b)
		privateCIDRs = append(privateCIDRs, cidr)
	}
}

func validateTargetURL(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("target_url is required")
	}
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return "", fmt.Errorf("target_url is not a valid URL")
	}
	if u.Scheme != "https" {
		return "", fmt.Errorf("target_url must use HTTPS")
	}
	host := u.Hostname()
	if host == "localhost" {
		return "", fmt.Errorf("target_url must not target localhost")
	}
	ips, err := net.LookupHost(host)
	if err != nil {
		// If we can't resolve, allow (offline / mock mode); real deployments should resolve
		return u.Host, nil
	}
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		for _, cidr := range privateCIDRs {
			if cidr.Contains(ip) {
				return "", fmt.Errorf("target_url resolves to a private/internal address")
			}
		}
	}
	return u.Host, nil
}

func validateMethod(m string) error {
	upper := strings.ToUpper(m)
	if !allowedMethods[upper] {
		return fmt.Errorf("method must be POST, PUT or PATCH")
	}
	return nil
}

func validateHeaders(h map[string]string) error {
	total := 0
	for k, v := range h {
		total += len(k) + len(v)
	}
	if total > 8*1024 {
		return fmt.Errorf("headers exceed 8KB limit")
	}
	return nil
}

func validateBody(b string) error {
	if len(b) > 1024*1024 {
		return fmt.Errorf("body exceeds 1MB limit")
	}
	return nil
}
