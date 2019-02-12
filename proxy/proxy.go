/*
Copyright 2016 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package proxy defines an App Engine app implementing an inverse proxy.
//
// To deploy, export the ID of your project as ${PROJECT_ID} and then run:
//
//    $ make deploy
package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"golang.org/x/net/context"
)

const (
	requestsWaitTimeout = 30 * time.Second
	responseWaitTimeout = 30 * time.Second

	// HeaderUserID is the HTTP response header used to report the end user.
	HeaderUserID = "X-Inverting-Proxy-User-ID"

	// HeaderRequestID is the HTTP request/response header used to uniquely identify an end-user request.
	HeaderRequestID = "X-Inverting-Proxy-Request-ID"

	// HeaderBackendID is the HTTP request/response header used to uniquely identify a proxied backend server.
	HeaderBackendID = "X-Inverting-Proxy-Backend-ID"

	// HeaderRequestStartTime is the HTTP response header used to report when an end-user request was initiated.
	HeaderRequestStartTime = "X-Inverting-Proxy-Request-Start-Time"
)

func waitForNextRequests(ctx context.Context, s Store, backendID string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, requestsWaitTimeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("Timeout waiting for requests for %q", backendID)
		default:
			if rs, err := s.ListPendingRequests(ctx, backendID); err == nil {
				if len(rs) > 0 {
					return rs, nil
				}
			} else {
				log.Printf("Error listing pending requests: %v", err)
			}
			// Wait a bit before checking again
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func waitForResponse(ctx context.Context, s Store, backendID, requestID string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, responseWaitTimeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("Timeout waiting for a response from %q", backendID)
		default:
			if r, err := s.ReadResponse(ctx, backendID, requestID); err == nil {
				if len(r.Contents) > 0 {
					return r.Contents, nil
				}
			}
			// Wait a bit before checking again
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func postRequest(ctx context.Context, s Store, backendID, requestID, userEmail string, requestBytes []byte) (*Request, error) {
	request := NewRequest(backendID, requestID, userEmail, requestBytes)
	if err := s.WriteRequest(ctx, request); err != nil {
		return nil, err
	}
	return request, nil
}

func parseResponse(backendID string, r *http.Request) (*Response, error) {
	requestID := r.Header.Get(HeaderRequestID)
	if requestID == "" {
		return nil, errors.New("No request ID specified")
	}
	responseBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return &Response{
		BackendID: backendID,
		RequestID: requestID,
		Contents:  responseBytes,
	}, nil
}

func postResponse(ctx context.Context, s Store, response *Response, errChan chan error) {
	backendID := response.BackendID
	requestID := response.RequestID
	log.Printf("Responding to request [%q] for the backend %q", requestID, backendID)
	request, err := s.ReadRequest(ctx, backendID, requestID)
	if err != nil {
		errChan <- fmt.Errorf("No matching request: %q", requestID)
		return
	}
	response.StartTime = request.StartTime
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := s.WriteResponse(ctx, response); err != nil {
			errChan <- err
		}
	}()
	go func() {
		defer wg.Done()
		request.Completed = true
		if err := s.WriteRequest(ctx, request); err != nil {
			errChan <- err
		}
	}()
	wg.Wait()
}

func checkBackendID(ctx context.Context, s Store, r *http.Request) (string, error) {
	backendUser := "standard"
	backendID := r.Header.Get(HeaderBackendID)
	if backendID == "" {
		return "", errors.New("No backend ID specified")
	}

	allowed, err := s.IsBackendUserAllowed(ctx, backendUser, backendID)
	if err != nil {
		return "", fmt.Errorf("Failed to look up backend user: %q", err.Error())
	}
	if !allowed {
		return "", errors.New("Backend user not allowed")
	}
	return backendID, nil
}

func pendingHandler(ctx context.Context, s Store, w http.ResponseWriter, r *http.Request) {
	backendID, err := checkBackendID(ctx, s, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	pendingRequests, err := waitForNextRequests(ctx, s, backendID)
	if err != nil {
		log.Printf("Found no new requests for %q: %q", backendID, err.Error())
		pendingRequests = []string{}
	}

	requestsBytes, err := json.Marshal(pendingRequests)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(requestsBytes)
}

func requestHandler(ctx context.Context, s Store, w http.ResponseWriter, r *http.Request) {
	backendID, err := checkBackendID(ctx, s, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	requestID := r.Header.Get(HeaderRequestID)
	if requestID == "" {
		errorMsg := "No request ID specified"
		http.Error(w, errorMsg, http.StatusBadRequest)
		return
	}
	w.Header().Add(HeaderRequestID, requestID)

	request, err := s.ReadRequest(ctx, backendID, requestID)
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to read the request for %q: %q", requestID, err.Error())
		http.Error(w, errorMsg, http.StatusNotFound)
		return
	}
	w.Header().Add(HeaderUserID, request.User)

	startTimeText := request.StartTime.Format(time.RFC3339Nano)
	w.Header().Add(HeaderRequestStartTime, startTimeText)
	w.Write(request.Contents)
}

func responseHandler(ctx context.Context, s Store, w http.ResponseWriter, r *http.Request) {
	backendID, err := checkBackendID(ctx, s, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	response, err := parseResponse(backendID, r)
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to read the response body: %q", err.Error())
		http.Error(w, errorMsg, http.StatusBadRequest)
		return
	}
	notFoundErrs := make(chan error, 1)
	log.Printf("Posting a response [%q]", response.RequestID)
	postResponse(ctx, s, response, notFoundErrs)
	close(notFoundErrs)
	if len(notFoundErrs) > 0 {
		notFoundErr := <-notFoundErrs
		http.Error(w, notFoundErr.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func deleteHandler(ctx context.Context, s Store, w http.ResponseWriter, r *http.Request) {
	log.Printf("Deleting old data")
	if err := s.DeleteOldBackends(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := s.DeleteOldRequests(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte{})
	return
}

func reportError(w http.ResponseWriter, requestID string, err error, statusCode int) {
	w.Header().Add(HeaderRequestID, requestID)
	http.Error(w, err.Error(), statusCode)
}

func forwardResponse(ctx context.Context, requestID string, w http.ResponseWriter, response *http.Response) {
	for k, v := range response.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(response.StatusCode)
	if _, err := io.Copy(w, response.Body); err != nil {
		log.Fatalf("Failed to copy a response: %v", err)
	}
}

func proxyHandler(ctx context.Context, s Store, requestID string, w http.ResponseWriter, r *http.Request) {
	userEmail := "standard"
	backendID, err := s.LookupBackend(ctx, userEmail, r.URL.Path)
	if err != nil {
		log.Printf("No matching backends for %q", userEmail)
		http.NotFound(w, r)
		return
	}

	// Note that App Engine has a safety limit on request sizes of 32MB
	// (see https://cloud.google.com/appengine/quotas#Requests), so
	// the following buffer cannot grow larger than that.
	var requestBuffer bytes.Buffer
	if err := r.Write(&requestBuffer); err != nil {
		reportError(w, requestID, err, http.StatusInternalServerError)
		return
	}

	requestBytes, err := ioutil.ReadAll(&requestBuffer)
	if err != nil {
		reportError(w, requestID, err, http.StatusInternalServerError)
		return
	}

	if _, err := postRequest(ctx, s, backendID, requestID, userEmail, requestBytes); err != nil {
		reportError(w, requestID, err, http.StatusInternalServerError)
		return
	}

	responseBytes, err := waitForResponse(ctx, s, backendID, requestID)
	if err != nil {
		reportError(w, requestID, err, http.StatusGatewayTimeout)
		return
	}
	responseReader := bufio.NewReader(bytes.NewReader(responseBytes))
	response, err := http.ReadResponse(responseReader, r)
	if err != nil {
		reportError(w, requestID, err, http.StatusInternalServerError)
		return
	}

	forwardResponse(ctx, requestID, w, response)
}

func parseBackend(r *http.Request) (*Backend, error) {
	requestBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	var backend Backend
	if err := json.Unmarshal(requestBytes, &backend); err != nil {
		return nil, err
	}
	return &backend, nil
}

func addBackendHandler(ctx context.Context, s Store, w http.ResponseWriter, r *http.Request) {
	backend, err := parseBackend(r)
	if err != nil {
		http.Error(w, "Failed to parse the backend definition", http.StatusBadRequest)
		return
	}
	if err := s.AddBackend(ctx, backend); err != nil {
		http.Error(w, "Failed to add a backend", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func listBackendsHandler(ctx context.Context, s Store, w http.ResponseWriter, r *http.Request) {
	backends, err := s.ListBackends(ctx)
	if err != nil {
		http.Error(w, "Failed to list backends", http.StatusInternalServerError)
		return
	}

	responseBytes, err := json.Marshal(backends)
	if err != nil {
		http.Error(w, "Failed to list backends", http.StatusInternalServerError)
		return
	}

	w.Write(responseBytes)
}

func deleteBackendHandler(ctx context.Context, s Store, w http.ResponseWriter, r *http.Request) {
	backendID := strings.TrimPrefix(r.URL.Path, "/api/backends/")
	if backendID == "" {
		http.Error(w, "No backend specified", http.StatusBadRequest)
		return
	}

	if err := s.DeleteBackend(ctx, backendID); err != nil {
		http.Error(w, "Failed to delete the backend", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleAgentRequest routes a request from a forwarding agent to the appropriate request handler.
func handleAgentRequest(ctx context.Context, s Store, w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/agent/pending") {
		pendingHandler(ctx, s, w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/agent/request") {
		requestHandler(ctx, s, w, r)
		return
	} else if strings.HasPrefix(r.URL.Path, "/agent/response") {
		responseHandler(ctx, s, w, r)
		return
	}
	http.Error(w, "Invalid request path", http.StatusNotFound)
	return
}

// handleAPIRequest routes an API request from an app administrator to the appropriate request handler.
func handleAPIRequest(ctx context.Context, s Store, w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/cron/delete" {
		// We don't check the user identity here as this path is restricted to admin users.
		deleteHandler(ctx, s, w, r)
		return
	}

	if r.URL.Path == "/api/backends" {
		if r.Method == http.MethodGet {
			listBackendsHandler(ctx, s, w, r)
			return
		} else if r.Method == http.MethodPost {
			addBackendHandler(ctx, s, w, r)
			return
		}
		http.Error(w, "Unsupported API method: "+r.Method, http.StatusMethodNotAllowed)
		return
	} else if strings.HasPrefix(r.URL.Path, "/api/backends/") {
		if r.Method == http.MethodDelete {
			deleteBackendHandler(ctx, s, w, r)
			return
		}
		http.Error(w, "Unsupported API method: "+r.Method, http.StatusMethodNotAllowed)
		return
	}

	http.Error(w, "Invalid request path", http.StatusNotFound)
	return
}

// NewProxy creates a new Proxy-HTTP-Server
func NewProxy(s Store) http.Handler {
	r := chi.NewRouter()

	// A good base middleware stack
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	r.HandleFunc("/agent/pending", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		log.Println("---------- " + r.Header.Get(HeaderBackendID))
		handleAgentRequest(ctx, s, w, r)
	})

	r.HandleFunc("/api/backends", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		handleAPIRequest(ctx, s, w, r)
	})

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		log.Println(">>>>>>>>>> ")
		ID := middleware.GetReqID(ctx)
		proxyHandler(ctx, s, ID, w, r)
	})

	return r
}
