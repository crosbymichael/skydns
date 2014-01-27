// Copyright (c) 2013 Erik St. Martin, Brian Ketelsen. All rights reserved.
// Use of this source code is governed by The MIT License (MIT) that can be
// found in the LICENSE file.

package server

import (
	"crypto/sha1"
	"github.com/miekg/dns"
	"log"
	"os"
	"sync"
	"time"
)

const origTTL uint32 = 3600

var cache *sigCache = newCache()
var inflight *single = new(single)

// ParseKeyFile read a DNSSEC keyfile as generated by dnssec-keygen or other
// utilities. It add ".key" for the public key and ".private" for the private key.
func ParseKeyFile(file string) (*dns.DNSKEY, dns.PrivateKey, error) {
	f, e := os.Open(file + ".key")
	if e != nil {
		return nil, nil, e
	}
	k, e := dns.ReadRR(f, file+".key")
	if e != nil {
		return nil, nil, e
	}
	f, e = os.Open(file + ".private")
	if e != nil {
		return nil, nil, e
	}
	p, e := k.(*dns.DNSKEY).ReadPrivateKey(f, file+".private")
	if e != nil {
		return nil, nil, e
	}
	return k.(*dns.DNSKEY), p, nil
}

// sign signs a message m, it takes care of negative or nodata responses as
// well by synthesising NSEC records. It will also cache the signatures, using
// a hash of the signed data as a key as well as the generated NSEC records.
// We also fake the origin TTL in the signature, because we don't want to
// throw away signatures when services decide to have longer TTL.
func (s *Server) sign(m *dns.Msg, bufsize uint16) {
	now := time.Now().UTC()
	incep := uint32(now.Add(-2 * time.Hour).Unix()) // 2 hours, be sure to catch daylight saving time and such
	expir := uint32(now.Add(7 * 24 * time.Hour).Unix())

	for _, r := range rrSets(m.Answer) {
		key := cache.key(r)
		if s := cache.search(key); s != nil {
			if s.ValidityPeriod(now.Add(-10 * time.Second)) {
				m.Answer = append(m.Answer, s)
				continue
			}
		}
		sig := s.newSig(incep, expir)
		if e := sig.Sign(s.Privkey, r); e != nil {
			log.Printf("Failed to sign: %s\n", e.Error())
			continue
		}
		cache.insert(key, sig)
		m.Answer = append(m.Answer, sig)
	}
	for _, r := range rrSets(m.Ns) {
		key := cache.key(r)
		if s := cache.search(key); s != nil {
			if s.ValidityPeriod(now.Add(-10 * time.Second)) {
				m.Answer = append(m.Answer, s)
				continue
			}
			m.Ns = append(m.Ns, s)
			continue
		}
		sig := s.newSig(incep, expir)
		if e := sig.Sign(s.Privkey, r); e != nil {
			log.Printf("Failed to sign: %s\n", e.Error())
			continue
		}
		m.Ns = append(m.Ns, sig)
	}
	// TODO(miek): Forget the additional section for now
	if bufsize >= 512 || bufsize <= 4096 {
		m.Truncated = m.Len() > int(bufsize)
	}
	o := new(dns.OPT)
	o.Hdr.Name = "."
	o.Hdr.Rrtype = dns.TypeOPT
	o.SetDo()
	o.SetUDPSize(4096)
	m.Extra = append(m.Extra, o)
	return
}

func (s *Server) newSig(incep, expir uint32) *dns.RRSIG {
	sig := new(dns.RRSIG)
	sig.Hdr.Ttl = origTTL
	sig.OrigTtl = origTTL
	sig.Algorithm = s.Dnskey.Algorithm
	sig.KeyTag = s.KeyTag
	sig.Inception = incep
	sig.Expiration = expir
	sig.SignerName = s.Dnskey.Hdr.Name
	return sig
}

type rrset struct {
	qname  string
	qclass uint16
	qtype  uint16
}

func rrSets(rrs []dns.RR) map[rrset][]dns.RR {
	m := make(map[rrset][]dns.RR)
	for _, r := range rrs {
		if s, ok := m[rrset{r.Header().Name, r.Header().Class, r.Header().Rrtype}]; ok {
			s = append(s, r)
		} else {
			s := make([]dns.RR, 1, 3)
			s[0] = r
			m[rrset{r.Header().Name, r.Header().Class, r.Header().Rrtype}] = s
		}
	}
	if len(m) > 0 {
		return m
	}
	return nil
}

type sigCache struct {
	sync.RWMutex
	m map[string]*dns.RRSIG
}

func newCache() *sigCache {
	c := new(sigCache)
	c.m = make(map[string]*dns.RRSIG)
	return c
}

func (c *sigCache) remove(s string) {
	delete(c.m, s)
}

func (c *sigCache) insert(s string, r *dns.RRSIG) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.m[s]; !ok {
		c.m[s] = r
	}
}

func (c *sigCache) search(s string) *dns.RRSIG {
	c.RLock()
	defer c.RUnlock()
	if s, ok := c.m[s]; ok {
		// we want to return a copy here, because if we didn't the RRSIG
		// could be removed by another goroutine before the packet containing
		// this signature is send out.
		log.Println("DNS Signature retrieved from cache")
		return dns.Copy(s).(*dns.RRSIG)
	}
	return nil
}

// key uses the name, type and rdata, which is serialized and then hashed as the
// key for the lookup
func (c *sigCache) key(rrs []dns.RR) string {
	h := sha1.New()
	i := []byte(rrs[0].Header().Name)
	i = append(i, packUint16(rrs[0].Header().Rrtype)...)
	for _, r := range rrs {
		switch t := r.(type) { // we only do a few type, serialize these manually
		case *dns.SOA:
			i = append(i, packUint32(t.Serial)...)
			// we only fiddle with the serial so store that
		case *dns.SRV:
			i = append(i, packUint16(t.Priority)...)
			i = append(i, packUint16(t.Weight)...)
			i = append(i, packUint16(t.Weight)...)
			i = append(i, []byte(t.Target)...)
		case *dns.A:
			i = append(i, []byte(t.A)...)
		case *dns.AAAA:
			i = append(i, []byte(t.AAAA)...)
		case *dns.DNSKEY:
			// Need nothing more, the rdata stays the same during a run
		case *dns.NSEC:
			// nextname?
		default:
			// not handled
		}
	}
	return string(h.Sum(i))
}

func packUint16(i uint16) []byte { return []byte{byte(i >> 8), byte(i)} }
func packUint32(i uint32) []byte { return []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)} }

// Adapted from singleinflight.go from the original Go Code. Copyright 2013 The Go Authors.
type call struct {
	wg   sync.WaitGroup
	val  *dns.RRSIG
	err  error
	dups int
}

type single struct {
	sync.Mutex
	m map[string]*call
}

func (g *single) Do(key string, fn func() (*dns.RRSIG, error)) (*dns.RRSIG, error, bool) {
	g.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.Unlock()
	return c.val, c.err, c.dups > 0
}
