package config

import (
	"net"
	"testing"
)

func TestPublicTraceURLConfigured(t *testing.T) {
	for url, want := range map[string]bool{
		"": false,
		"http://localhost:8080/api/v1/traceability/public/q": false,
		"http://127.0.0.1:4011/public/q":                     false,
		"https://trace.iag.coffee/q":                         true,
	} {
		if got := (&Config{PublicTraceBaseURL: url}).PublicTraceURLConfigured(); got != want {
			t.Errorf("%q: %v", url, got)
		}
	}
	if !(&Config{Environment: "development"}).LocalEnvironment() || (&Config{Environment: "production"}).LocalEnvironment() {
		t.Fatal("local vs deployed")
	}
}

func TestPrivateNetworksAreTheOnlyDefaultProxies(t *testing.T) {
	inRange := func(ip string) bool {
		for _, cidr := range PrivateNetworks {
			_, n, err := net.ParseCIDR(cidr)
			if err != nil {
				t.Fatal(err)
			}
			if n.Contains(net.ParseIP(ip)) {
				return true
			}
		}
		return false
	}
	if !inRange("10.0.4.7") || !inRange("fd12::1") {
		t.Fatal("the platform's internal network is trusted")
	}
	if inRange("41.210.143.9") || inRange("8.8.8.8") {
		t.Fatal("a public client must never be trusted to name its own address")
	}
}
