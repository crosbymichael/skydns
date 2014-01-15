// Copyright (c) 2013 Erik St. Martin, Brian Ketelsen. All rights reserved.
// Use of this source code is governed by The MIT License (MIT) that can be
// found in the LICENSE file.

package v1

import (
	"bytes"
	"encoding/json"
	"github.com/skynetservices/skydns/registry"
	"log"
	"net/http"
)

func GetRegionsHTTPHandler(w http.ResponseWriter, req *http.Request, s Server) {
	//read the authorization header to get the secret.
	secret := req.Header.Get("Authorization")

	if err := s.Authenticate(secret); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	srv, err := s.Get("*")
	if err != nil {
		switch err {
		case registry.ErrNotExists:
			w.Write([]byte("{}"))
		default:
			log.Println("Error: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	regions := make(map[string]int, 1)

	for _, service := range srv {
		if _, ok := regions[service.Region]; ok {
			// exists, increment
			regions[service.Region] = regions[service.Region] + 1
		} else {
			regions[service.Region] = 1
		}
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(regions)
	w.Write(b.Bytes())

}

func GetEnvironmentsHTTPHandler(w http.ResponseWriter, req *http.Request, s Server) {
	//read the authorization header to get the secret.
	secret := req.Header.Get("Authorization")

	if err := s.Authenticate(secret); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	srv, err := s.Get("*")
	if err != nil {
		switch err {
		case registry.ErrNotExists:
			w.Write([]byte("{}"))
		default:
			log.Println("Error: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	environments := make(map[string]int, 1)

	for _, service := range srv {
		if _, ok := environments[service.Environment]; ok {
			// exists, increment
			environments[service.Environment] = environments[service.Environment] + 1
		} else {
			environments[service.Environment] = 1
		}
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(environments)
	w.Write(b.Bytes())
}

func GetServicesHTTPHandler(w http.ResponseWriter, req *http.Request, s Server) {
	//read the authorization header to get the secret.
	secret := req.Header.Get("Authorization")

	if err := s.Authenticate(secret); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	log.Println(req.URL.Path)
	log.Println(s.Leader())

	var q string

	if q = req.URL.Query().Get("query"); q == "" {
		q = "*"
	}

	log.Println("Retrieving All Services for query", q)

	srv, err := s.Get(q)

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
	json.NewEncoder(&b).Encode(srv)
	w.Write(b.Bytes())
}
