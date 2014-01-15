// Copyright (c) 2013 Erik St. Martin, Brian Ketelsen. All rights reserved.
// Use of this source code is governed by The MIT License (MIT) that can be
// found in the LICENSE file.

package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"github.com/miekg/dns"
	"github.com/skynetservices/skydns/msg"
	"github.com/skynetservices/skydns/registry"
	"github.com/skynetservices/skydns/server/v1"
	"github.com/skynetservices/skydns/stats"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

/* TODO:
   Set Priority based on Region
   Dynamically set Weight/Priority in DNS responses
   Handle API call for setting host statistics
   Handle Errors in DNS
   Master should cleanup expired services
   TTL cleanup thread should shutdown/start based on being elected master
*/

func init() {
	// Register Raft Commands
	raft.RegisterCommand(&AddServiceCommand{})
	raft.RegisterCommand(&UpdateTTLCommand{})
	raft.RegisterCommand(&RemoveServiceCommand{})
	raft.RegisterCommand(&AddCallbackCommand{})
}

type Server struct {
	members      []string // initial members to join with
	domain       string
	dnsAddr      string
	httpAddr     string
	readTimeout  time.Duration
	writeTimeout time.Duration
	waiter       *sync.WaitGroup

	registry registry.Registry

	dnsUDPServer *dns.Server
	dnsTCPServer *dns.Server
	dnsHandler   *dns.ServeMux

	httpServer *http.Server
	router     *mux.Router

	raftServer raft.Server
	dataDir    string
	secret     string
}

// Newserver returns a new Server.
func NewServer(members []string, domain string, dnsAddr string, httpAddr string, dataDir string, rt, wt time.Duration, secret string) (s *Server) {
	s = &Server{
		members:      members,
		domain:       domain,
		dnsAddr:      dnsAddr,
		httpAddr:     httpAddr,
		readTimeout:  rt,
		writeTimeout: wt,
		router:       mux.NewRouter(),
		registry:     registry.New(),
		dataDir:      dataDir,
		dnsHandler:   dns.NewServeMux(),
		waiter:       new(sync.WaitGroup),
		secret:       secret,
	}

	if _, err := os.Stat(s.dataDir); os.IsNotExist(err) {
		log.Fatal("Data directory does not exist: ", dataDir)
		return
	}

	// DNS
	s.dnsHandler.Handle(".", s)

	// API Routes
	s.handleFuncv1("/skydns/services/{uuid}", "PUT", v1.AddServiceHTTPHandler)
	s.handleFuncv1("/skydns/services/{uuid}", "GET", v1.GetServiceHTTPHandler)
	s.handleFuncv1("/skydns/services/{uuid}", "DELETE", v1.RemoveServiceHTTPHandler)
	s.handleFuncv1("/skydns/services/{uuid}", "PATCH", v1.UpdateServiceHTTPHandler)

	s.handleFuncv1("/skydns/callbacks/{uuid}", "PUT", v1.AddCallbackHTTPHandler)

	// External API Routes
	// /skydns/services #list all services
	s.handleFuncv1("/skydns/services/", "GET", v1.GetServicesHTTPHandler)
	// /skydns/regions #list all regions
	s.handleFuncv1("/skydns/regions/", "GET", v1.GetRegionsHTTPHandler)
	// /skydns/environnments #list all environments
	s.handleFuncv1("/skydns/environments/", "GET", v1.GetEnvironmentsHTTPHandler)

	// Raft Routes
	s.router.HandleFunc("/raft/join", s.joinHandler).Methods("POST")

	return
}

// DNSAddr returns IP:Port of a DNS Server.
func (s *Server) DNSAddr() string { return s.dnsAddr }

// HTTPAddr returns IP:Port of HTTP Server.
func (s *Server) HTTPAddr() string { return s.httpAddr }

// Start starts a DNS server and blocks waiting to be killed.
func (s *Server) Start() (*sync.WaitGroup, error) {
	var err error
	log.Printf("Initializing Server. DNS Addr: %q, HTTP Addr: %q, Data Dir: %q", s.dnsAddr, s.httpAddr, s.dataDir)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft")
	s.raftServer, err = raft.NewServer(s.HTTPAddr(), s.dataDir, transporter, nil, s.registry, "")
	if err != nil {
		log.Fatal(err)
	}
	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	// Join to leader if specified.
	if len(s.members) > 0 {
		log.Println("Joining cluster:", strings.Join(s.members, ","))

		if !s.raftServer.IsLogEmpty() {
			log.Fatal("Cannot join with an existing log")
		}

		if err := s.Join(s.members); err != nil {
			return nil, err
		}

		log.Println("Joined cluster")

		// Initialize the server by joining itself.
	} else if s.raftServer.IsLogEmpty() {
		log.Println("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString(),
		})

		if err != nil {
			log.Fatal(err)
			return nil, err
		}

	} else {
		log.Println("Recovered from log")
	}

	s.dnsTCPServer = &dns.Server{
		Addr:         s.DNSAddr(),
		Net:          "tcp",
		Handler:      s.dnsHandler,
		ReadTimeout:  s.readTimeout,
		WriteTimeout: s.writeTimeout,
	}

	s.dnsUDPServer = &dns.Server{
		Addr:         s.DNSAddr(),
		Net:          "udp",
		Handler:      s.dnsHandler,
		UDPSize:      65535,
		ReadTimeout:  s.readTimeout,
		WriteTimeout: s.writeTimeout,
	}

	s.httpServer = &http.Server{
		Addr:           s.HTTPAddr(),
		Handler:        s.router,
		ReadTimeout:    s.readTimeout,
		WriteTimeout:   s.writeTimeout,
		MaxHeaderBytes: 1 << 20,
	}

	go s.listenAndServe()

	s.waiter.Add(1)
	go s.run()

	return s.waiter, nil
}

// Stop stops a server.
func (s *Server) Stop() {
	log.Println("Stopping server")
	s.waiter.Done()
}

// Leader returns the current leader.
func (s *Server) Leader() string {
	l := s.raftServer.Leader()

	if l == "" {
		// We are a single node cluster, we are the leader
		return s.raftServer.Name()
	}

	return l
}

// IsLeader returns true if this instance the current leader.
func (s *Server) IsLeader() bool {
	return s.raftServer.State() == raft.Leader
}

// Members returns the current members.
func (s *Server) Members() (members []string) {
	peers := s.raftServer.Peers()

	for _, p := range peers {
		members = append(members, strings.TrimPrefix(p.ConnectionString, "http://"))
	}

	return
}

func (s *Server) run() {
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	tick := time.Tick(1 * time.Second)

run:
	for {
		select {
		case <-tick:
			// We are the leader, we are responsible for managing TTLs
			if s.IsLeader() {
				expired := s.registry.GetExpired()

				// TODO: Possible race condition? We could be demoted while iterating
				// probably minimal chance of this happening, this will just cause commands to fail,
				// and new leader will take over anyway
				for _, uuid := range expired {
					stats.ExpiredCount.Inc(1)
					s.raftServer.Do(NewRemoveServiceCommand(uuid))
				}
			}
		case <-sig:
			break run
		}
	}
	s.Stop()
}

// Join joins an existing SkyDNS cluster.
func (s *Server) Join(members []string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString(),
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)

	for _, m := range members {
		log.Println("Attempting to connect to:", m)

		resp, err := http.Post(fmt.Sprintf("http://%s/raft/join", strings.TrimSpace(m)), "application/json", &b)
		log.Println("Post returned")

		if err != nil {
			if _, ok := err.(*url.Error); ok {
				// If we receive a network error try the next member
				continue
			}

			return err
		}

		resp.Body.Close()
		return nil
	}

	return errors.New("Could not connect to any cluster members")
}

// HandleFunc proxies HTTP handlers to Gorilla's mux.Router.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Handles incoming RAFT joins.
func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	log.Println("Processing incoming join")
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		log.Println("Error decoding json message:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := s.raftServer.Do(command); err != nil {
		switch err {
		case raft.NotLeaderError:
			log.Println("Redirecting to leader")
			v1.RedirectToLeader(w, req, s)
		default:
			log.Println("Error processing join:", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// Handler for DNS requests, responsible for parsing DNS request and returning response.
func (s *Server) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	stats.RequestCount.Inc(1)

	m := new(dns.Msg)
	m.SetReply(req)
	m.Answer = make([]dns.RR, 0, 10)

	defer w.WriteMsg(m)

	q := req.Question[0]

	log.Printf("Received DNS Request for %q from %q", q.Name, w.RemoteAddr())

	if q.Qtype == dns.TypeANY || q.Qtype == dns.TypeSRV {
		records, extra, err := s.getSRVRecords(q)

		if err != nil {
			m.SetRcode(req, dns.RcodeServerFailure)
			log.Println("Error: ", err)
			return
		}

		m.Answer = append(m.Answer, records...)
		m.Extra = append(m.Extra, extra...)
	}

	if q.Qtype == dns.TypeANY || q.Qtype == dns.TypeA {
		records, err := s.getARecords(q)

		if err != nil {
			m.SetRcode(req, dns.RcodeServerFailure)
			log.Println("Error: ", err)
			return
		}

		m.Answer = append(m.Answer, records...)
	}
}

func (s *Server) getARecords(q dns.Question) (records []dns.RR, err error) {
	var h string
	name := strings.TrimSuffix(q.Name, ".")

	// Leader should always be listed
	if name == "leader."+s.domain || name == "master."+s.domain || name == s.domain {
		h, _, err = net.SplitHostPort(s.Leader())

		if err != nil {
			return
		}

		records = append(records, &dns.A{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 15}, A: net.ParseIP(h)})
	}

	if name == s.domain {
		for _, m := range s.Members() {
			h, _, err = net.SplitHostPort(m)

			if err != nil {
				return
			}

			records = append(records, &dns.A{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 15}, A: net.ParseIP(h)})
		}
	}

	return
}

func (s *Server) getSRVRecords(q dns.Question) (records []dns.RR, extra []dns.RR, err error) {
	var weight uint16
	services := make([]msg.Service, 0)

	key := strings.TrimSuffix(q.Name, s.domain+".")
	services, err = s.registry.Get(key)

	if err != nil {
		return
	}

	weight = 0
	if len(services) > 0 {
		weight = uint16(math.Floor(float64(100 / len(services))))
	}

	for _, serv := range services {
		// TODO: Dynamically set weight
		// a Service may have an IP as its Host"name", in this case
		// substitute UUID + "." + s.domain+"." an add an A record
		// with the name and IP in the additional section.
		// TODO(miek): check if resolvers actually grok this
		ip := net.ParseIP(serv.Host)
		switch {
		case ip == nil:
			records = append(records, &dns.SRV{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: serv.TTL},
				Priority: 10, Weight: weight, Port: serv.Port, Target: serv.Host + "."})
			continue
		case ip.To4() != nil:
			extra = append(extra, &dns.A{Hdr: dns.RR_Header{Name: serv.UUID + "." + s.domain + ".", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: serv.TTL}, A: ip.To4()})
			records = append(records, &dns.SRV{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: serv.TTL},
				Priority: 10, Weight: weight, Port: serv.Port, Target: serv.UUID + "." + s.domain + "."})
		case ip.To16() != nil:
			extra = append(extra, &dns.AAAA{Hdr: dns.RR_Header{Name: serv.UUID + "." + s.domain + ".", Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: serv.TTL}, AAAA: ip.To16()})
			records = append(records, &dns.SRV{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: serv.TTL},
				Priority: 10, Weight: weight, Port: serv.Port, Target: serv.UUID + "." + s.domain + "."})
		default:
			panic("skydns: internal error")
		}
	}

	// Append matching entries in different region than requested with a higher priority
	labels := dns.SplitDomainName(key)

	pos := len(labels) - 4
	if len(labels) >= 4 && labels[pos] != "*" {
		region := labels[pos]
		labels[pos] = "*"

		// TODO: This is pretty much a copy of the above, and should be abstracted
		additionalServices := make([]msg.Service, len(services))
		additionalServices, err = s.registry.Get(strings.Join(labels, "."))

		if err != nil {
			return
		}

		weight = 0
		if len(additionalServices) <= len(services) {
			return
		}

		weight = uint16(math.Floor(float64(100 / (len(additionalServices) - len(services)))))
		for _, serv := range additionalServices {
			// Exclude entries we already have
			if strings.ToLower(serv.Region) == region {
				continue
			}
			// TODO: Dynamically set priority and weight
			// TODO(miek): same as above: abstract away
			ip := net.ParseIP(serv.Host)
			switch {
			case ip == nil:
				records = append(records, &dns.SRV{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: serv.TTL},
					Priority: 20, Weight: weight, Port: serv.Port, Target: serv.Host + "."})
				continue
			case ip.To4() != nil:
				extra = append(extra, &dns.A{Hdr: dns.RR_Header{Name: serv.UUID + "." + s.domain + ".", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: serv.TTL}, A: ip.To4()})
				records = append(records, &dns.SRV{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: serv.TTL},
					Priority: 20, Weight: weight, Port: serv.Port, Target: serv.UUID + "." + s.domain + "."})
			case ip.To16() != nil:
				extra = append(extra, &dns.AAAA{Hdr: dns.RR_Header{Name: serv.UUID + "." + s.domain + ".", Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: serv.TTL}, AAAA: ip.To16()})
				records = append(records, &dns.SRV{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: serv.TTL},
					Priority: 20, Weight: weight, Port: serv.Port, Target: serv.UUID + "." + s.domain + "."})
			default:
				panic("skydns: internal error")
			}
		}
	}
	return
}

// Returns the connection string.
func (s *Server) connectionString() string {
	return fmt.Sprintf("http://%s", s.httpAddr)
}

// Binds to DNS and HTTP ports and starts accepting connections
func (s *Server) listenAndServe() {
	go func() {
		err := s.dnsTCPServer.ListenAndServe()
		if err != nil {
			log.Fatalf("Start %s listener on %s failed:%s", s.dnsTCPServer.Net, s.dnsTCPServer.Addr, err.Error())
		}
	}()

	go func() {
		err := s.dnsUDPServer.ListenAndServe()
		if err != nil {
			log.Fatalf("Start %s listener on %s failed:%s", s.dnsUDPServer.Net, s.dnsUDPServer.Addr, err.Error())
		}
	}()

	go func() {
		err := s.httpServer.ListenAndServe()
		if err != nil {
			log.Fatalf("Start http listener on %s failed:%s", s.httpServer.Addr, err.Error())
		}
	}()
}

// shared auth method on server.
func (s *Server) Authenticate(secret string) (err error) {
	if s.secret != "" && secret != s.secret {
		err = errors.New("Forbidden")
	}
	return
}

// AddService adds a new service to the server
func (s *Server) AddService(serv msg.Service) error {
	if _, err := s.raftServer.Do(NewAddServiceCommand(serv)); err != nil {
		return err
	}
	return nil
}

// RemoveService removes the service provided by uuid from the server
func (s *Server) RemoveService(uuid string) error {
	if _, err := s.raftServer.Do(NewRemoveServiceCommand(uuid)); err != nil {
		return err
	}
	return nil
}

// UpdateTTL updates the service from provided by uuid with the provided ttl value
func (s *Server) UpdateTTL(uuid string, ttl uint32) error {
	if _, err := s.raftServer.Do(NewUpdateTTLCommand(uuid, ttl)); err != nil {
		return err
	}
	return nil
}

// GetUUID returns the service from the registry provided by the uuid
func (s *Server) GetUUID(uuid string) (msg.Service, error) {
	return s.registry.GetUUID(uuid)
}

// Get returns the services provided by the key
func (s *Server) Get(key string) ([]msg.Service, error) {
	return s.registry.Get(key)
}

// Callback sends to service and cb to the server
func (s *Server) Callback(serv msg.Service, cb msg.Callback) error {
	if _, err := s.raftServer.Do(NewAddCallbackCommand(serv, cb)); err != nil {
		return err
	}
	return nil
}

func (s *Server) handleFuncv1(path, method string, handler func(http.ResponseWriter, *http.Request, v1.Server)) {
	f := func(w http.ResponseWriter, r *http.Request) {
		handler(w, r, s)
	}
	for _, p := range []string{
		path,
		fmt.Sprintf("/v1%s", path),
	} {
		s.router.HandleFunc(p, f).Methods(method)
	}
}
