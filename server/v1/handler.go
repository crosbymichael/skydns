package v1

import (
	"bytes"
	"encoding/json"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"github.com/skynetservices/skydns/msg"
	"github.com/skynetservices/skydns/registry"
	"github.com/skynetservices/skydns/stats"
	"log"
	"net/http"
)

type Server interface {
	AddService(msg.Service) error
	RemoveService(uuid string) error
	UpdateTTL(uuid string, ttl uint32) error
	Get(key string) ([]msg.Service, error)
	Callback(serv msg.Service, cb msg.Callback) error
	Leader() string
	Authenticate(secret string) error
	GetUUID(uuid string) (msg.Service, error)
}

// Handle API add service requests
func AddServiceHTTPHandler(w http.ResponseWriter, req *http.Request, s Server) {
	stats.AddServiceCount.Inc(1)

	var (
		uuid string
		ok   bool
		vars = mux.Vars(req)

		//read the authorization header to get the secret.
		secret = req.Header.Get("Authorization")
	)

	if err := s.Authenticate(secret); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	if uuid, ok = vars["uuid"]; !ok {
		http.Error(w, "UUID required", http.StatusBadRequest)
		return
	}

	var serv msg.Service

	if err := json.NewDecoder(req.Body).Decode(&serv); err != nil {
		log.Println("Error: ", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if serv.Host == "" || serv.Port == 0 {
		http.Error(w, "Host and Port required", http.StatusBadRequest)
		return
	}

	serv.UUID = uuid

	if err := s.AddService(serv); err != nil {
		switch err {
		case registry.ErrExists:
			http.Error(w, err.Error(), http.StatusConflict)
		case raft.NotLeaderError:
			RedirectToLeader(w, req, s)
		default:
			log.Println("Error: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	w.WriteHeader(http.StatusCreated)
}

// Handle API remove service requests
func RemoveServiceHTTPHandler(w http.ResponseWriter, req *http.Request, s Server) {
	stats.RemoveServiceCount.Inc(1)

	var (
		uuid string
		ok   bool

		vars = mux.Vars(req)

		//read the authorization header to get the secret.
		secret = req.Header.Get("Authorization")
	)

	if err := s.Authenticate(secret); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	if uuid, ok = vars["uuid"]; !ok {
		http.Error(w, "UUID required", http.StatusBadRequest)
		return
	}

	if err := s.RemoveService(uuid); err != nil {
		switch err {
		case registry.ErrNotExists:
			http.Error(w, err.Error(), http.StatusNotFound)
		case raft.NotLeaderError:
			RedirectToLeader(w, req, s)
		default:
			log.Println("Error: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// Handle API get service requests
func GetServiceHTTPHandler(w http.ResponseWriter, req *http.Request, s Server) {
	stats.GetServiceCount.Inc(1)

	var (
		uuid string
		ok   bool
		vars = mux.Vars(req)

		//read the authorization header to get the secret.
		secret = req.Header.Get("Authorization")
	)

	if err := s.Authenticate(secret); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	if uuid, ok = vars["uuid"]; !ok {
		http.Error(w, "UUID required", http.StatusBadRequest)
		return
	}

	log.Println("Retrieving Service ", uuid)
	serv, err := s.GetUUID(uuid)

	if err != nil {
		switch err {
		case registry.ErrNotExists:
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			log.Println("Error: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(serv)
	w.Write(b.Bytes())
}

// Handle API update service requests
func UpdateServiceHTTPHandler(w http.ResponseWriter, req *http.Request, s Server) {
	stats.UpdateTTLCount.Inc(1)

	var (
		uuid string
		ok   bool
		vars = mux.Vars(req)

		//read the authorization header to get the secret.
		secret = req.Header.Get("Authorization")
	)

	if err := s.Authenticate(secret); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	if uuid, ok = vars["uuid"]; !ok {
		http.Error(w, "UUID required", http.StatusBadRequest)
		return
	}

	var serv msg.Service
	if err := json.NewDecoder(req.Body).Decode(&serv); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := s.UpdateTTL(uuid, serv.TTL); err != nil {
		switch err {
		case registry.ErrNotExists:
			http.Error(w, err.Error(), http.StatusNotFound)
		case raft.NotLeaderError:
			RedirectToLeader(w, req, s)
		default:
			log.Println("Error: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func RedirectToLeader(w http.ResponseWriter, req *http.Request, s Server) {
	if s.Leader() != "" {
		http.Redirect(w, req, "http://"+s.Leader()+req.URL.Path, http.StatusMovedPermanently)
	} else {
		log.Println("Error: Leader Unknown")
		http.Error(w, "Leader unknown", http.StatusInternalServerError)
	}
}
