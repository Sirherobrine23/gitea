package setting

import (
	"context"
	"fmt"
	stdlog "log"
	"net/http"
	"net/netip"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"gitea.dev/modules/json"
	"gitea.dev/modules/log"
	"sirherobrine23.com.br/Sirherobrine23/whois"
)

type BlockerConfigContext string

type BlockerConfig struct {
	CIDR   []string `ini:"cidr" delim:"," json:"cidr"`
	ASN    []string `ini:"asn" delim:"," json:"asn"`
	Header []string `ini:"-" json:"headers"`
}

const (
	BlockConfigValue BlockerConfigContext = "config-block"
)

var (
	glbBlockMu   sync.RWMutex
	glbRegex     = map[*regexp.Regexp]struct{}{}
	glbPrefix    = map[netip.Prefix]struct{}{}
	BlockConfigs = [2]*BlockerConfig{}
)

func ProcessBlockConfig() {
	for {
		nextRegex := make(map[*regexp.Regexp]struct{})
		nextPrefix := make(map[netip.Prefix]struct{})

		log.Info("Updating Block configs")
		for _, BlockConfig := range BlockConfigs {
			if BlockConfig == nil {
				continue
			}
			if len(BlockConfig.CIDR) > 0 {
				for index, raw := range BlockConfig.CIDR {
					prefix, err := netip.ParsePrefix(raw)
					if err != nil {
						stdlog.Fatalf("error parse CIDR in %d %q, err: %s", index, raw, err)
					}
					nextPrefix[prefix] = struct{}{}
					log.Debug("CIDR for block: %s", prefix.String())
				}
			}

			if len(BlockConfig.ASN) > 0 {
				ctx := context.Background()
				for _, query := range BlockConfig.ASN {
					query = strings.ToUpper(query)

					switch v := strings.TrimFunc(query, func(r rune) bool { return !unicode.IsUpper(r) }); v {
					case "ASN":
						log.Info("Loading ip address for ASN: %q", query)
						if result, err := whois.RDAPLookup(ctx, query); err == nil {
							cidrs, err := result.Normalize().GetCIDRs()
							if err != nil {
								stdlog.Fatalf("RDAP: cannot get ASN cidrs: %s", err)
							}

							for _, prefix := range cidrs {
								nextPrefix[prefix] = struct{}{}
								log.Debug("CIDR for %q: %s", query, prefix.String())
							}
							continue
						}

						if result, err := whois.Lookup(ctx, query); err == nil {
							cidrs, err := result.Normalize().GetCIDRs()
							if err != nil {
								stdlog.Fatalf("WHOIS: cannot get ASN cidrs: %s", err)
							}

							for _, prefix := range cidrs {
								nextPrefix[prefix] = struct{}{}
								log.Debug("CIDR for %q: %s", query, prefix.String())
							}
							continue
						}
					case "AS":
						log.Info("Loading ip address for AS: %q", query)
						isAddr := func(r rune) bool {
							return !(unicode.IsNumber(r) || unicode.IsLetter(r) || r == ':' || r == '/' || r == '.')
						}
						if response, err := whois.Query(ctx, "whois.radb.net", fmt.Sprintf("!g%s", query)); err == nil {
							for p := range strings.FieldsFuncSeq(response.Body, isAddr) {
								if p != "" {
									if cidr, err := netip.ParsePrefix(p); err == nil && cidr.IsValid() {
										nextPrefix[cidr] = struct{}{}
										log.Debug("CIDR for %q: %s", query, cidr.String())
									}
								}
							}
						}

						if response, err := whois.Query(context.Background(), "whois.ripe.net", fmt.Sprintf("-i origin %s", query)); err == nil {
							for line := range strings.SplitSeq(response.Body, "\n") {
								line = strings.TrimSpace(line)
								if routev4, okv4 := strings.CutPrefix(line, "route:"); okv4 {
									for p := range strings.FieldsFuncSeq(routev4, isAddr) {
										if p != "" {
											if cidr, err := netip.ParsePrefix(p); err == nil && cidr.IsValid() {
												nextPrefix[cidr] = struct{}{}
												log.Debug("CIDR for %q: %s", query, cidr.String())
											}
										}
									}
								}
								if routev6, okv6 := strings.CutPrefix(line, "route6:"); okv6 {
									for p := range strings.FieldsFuncSeq(routev6, isAddr) {
										if p != "" {
											if cidr, err := netip.ParsePrefix(p); err == nil && cidr.IsValid() {
												nextPrefix[cidr] = struct{}{}
												log.Debug("CIDR for %q: %s", query, cidr.String())
											}
										}
									}
								}
							}
						}
					default:
						log.Warn("invalid value: %q", v)
					}
				}
			}

			if len(BlockConfig.Header) > 0 {
				for index, h := range BlockConfig.Header {
					rg, err := regexp.Compile(h)
					if err != nil {
						stdlog.Fatalf("error on test header regex in %d: %q", index, h)
					}
					nextRegex[rg] = struct{}{}
				}
			}
		}

		// Publish a complete immutable snapshot. Readers only hold the lock
		// while checking a request, and the slow RDAP/WHOIS refresh never blocks
		// HTTP handlers.
		glbBlockMu.Lock()
		glbRegex = nextRegex
		glbPrefix = nextPrefix
		glbBlockMu.Unlock()

		blockNextUpdate := time.Now().Add(time.Hour * 2)
		log.Info("Next update %s, until %s", blockNextUpdate, time.Until(blockNextUpdate))
		<-time.After(time.Until(blockNextUpdate))
	}
}

func loadBlockConfig(rootCfg ConfigProvider) {
	var BlockConfig BlockerConfig
	sec := rootCfg.Section("block")
	sec.MapTo(&BlockConfig)
	BlockConfigs[0] = &BlockConfig

	if sec.HasKey("CONFIG_FILE") {
		var BlockConfig BlockerConfig
		if f, err := os.Open(sec.Key("CONFIG_FILE").String()); err == nil {
			defer f.Close()
			if err := json.NewDecoder(f).Decode(&BlockConfig); err == nil {
				BlockConfigs[1] = &BlockConfig
			}
		}
	}
}

// Get blocker and filter
func IsBlock(r *http.Request) bool {
	userAgent := r.Header.Get("User-Agent")
	if userAgent == "" { // Always block without user-agent header
		return true
	}

	glbBlockMu.RLock()
	defer glbBlockMu.RUnlock()

	if addr := r.Header.Get("X-Real-IP"); addr != "" {
		if addrIP, err := netip.ParseAddrPort(addr); err == nil {
			for prefix := range glbPrefix {
				if prefix.Contains(addrIP.Addr()) {
					return true
				}
			}
		}
	}

	if addr := r.RemoteAddr; addr != "" {
		if addrIP, err := netip.ParseAddrPort(addr); err == nil {
			for prefix := range glbPrefix {
				if prefix.Contains(addrIP.Addr()) {
					return true
				}
			}
		}
	}

	for r := range glbRegex {
		if r.MatchString(userAgent) {
			return true
		}
	}

	return false
}
