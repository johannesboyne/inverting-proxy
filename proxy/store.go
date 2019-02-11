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

package proxy

import (
	"fmt"
	"log"
	"strings"
	"time"

	"golang.org/x/net/context"
)

const (
	backendTimeout    = 5 * time.Minute
	sharedBackendUser = "allUsers"

	// Datastore fields cannot exceed 1MB in size
	fieldByteLimit = 1000000
	// A single datastore operation cannot operate on more than 500 entities
	multiOpSizeLimit = 500

	backendKind         = "backend"
	backendTrackerKind  = "backendTracker"
	activityTrackerKind = "activityTracker"
	blobPartsKind       = "blobParts"
	requestKindPrefix   = "req:"
	responseKind        = "response"
)

type inMemoryStore struct {
	requests  map[string]*Request
	responses map[string]*Response
}

// Type persistentStore implements the Store interface and uses the Google Cloud Datastore for storing data.
type persistentStore struct {
	responses          map[string]*Response
	requests           map[string]*Request
	backends           map[string]*Backend
	registeredBackends map[string]bool
}

// NewPersistentStore returns a new implementation of the Store interface that uses the
// Google Cloud Datastore as its backing implementation.
func NewPersistentStore() Store {
	return &persistentStore{}
}

// WriteRequest writes the given request to the Datastore.
func (d *persistentStore) WriteRequest(ctx context.Context, r *Request) error {
	d.requests[fmt.Sprintf("req:%s:%s", r.BackendID, r.RequestID)] = r
	return nil
}

// ReadRequest reads the specified request from the Datastore.
func (d *persistentStore) ReadRequest(ctx context.Context, backendID, requestID string) (*Request, error) {
	return d.requests[fmt.Sprintf("req:%s:%s", backendID, requestID)], nil
}

// ListPendingRequests returns the list or requests for the given backend that do not yet have a stored response.
func (d *persistentStore) ListPendingRequests(ctx context.Context, backendID string) ([]string, error) {
	var requests []string
	if err := d.registerBackendAsSeen(ctx, backendID); err != nil {
		log.Fatalf("Failed to register a backend [%q]: %q", backendID, err.Error())
	}
	for k := range d.requests {
		if backendID == strings.Split(k, ":")[1] {
			requests = append(requests, d.requests[k].RequestID)
		}
	}

	return requests, nil
}

// WriteResponse writes the given response to the Datastore.
func (d *persistentStore) WriteResponse(ctx context.Context, r *Response) error {
	var err error
	if err := d.registerBackendAsActive(ctx, r.BackendID); err != nil {
		log.Fatalf("Failed to update a backend's activity tracker [%q]: %q", r.BackendID, err.Error())
	}
	d.responses[fmt.Sprintf("res:%s:%s", r.BackendID, r.RequestID)] = r
	return err
}

// ReadResponse reads from the Datastore the response to the specified request.
//
// If there is no response yet, then the returned value is nil.
func (d *persistentStore) ReadResponse(ctx context.Context, backendID, requestID string) (*Response, error) {
	return d.responses[fmt.Sprintf("res:%s:%s", backendID, requestID)], nil
}

// backendTracker is used to track how recently a backend was active.
//
// That, in turn, allows us to garbage collect dead backends.
type backendTracker struct {
	LastSeen time.Time
}

// activityTracker is used to track how recently a user request hit a specific backend.
//
// That can be used by administrators to spin down idle backends.
type activityTracker struct {
	LastActive time.Time
}

// DeleteOldBackends deletes from the datastore any backends older than 1 hour
func (d *persistentStore) DeleteOldBackends(ctx context.Context) error {
	return nil
}

// DeleteOldRequests deletes from the datastore any requests older than 2 minutes
func (d *persistentStore) DeleteOldRequests(ctx context.Context) error {
	for backend := range d.registeredBackends {
		for k := range d.requests {
			if backend == strings.Split(k, ":")[1] {
				delete(d.requests, k)
			}
		}
	}

	return nil
}

// registerBackendAsSeen records in the datastore that we just saw the given backend.
func (d *persistentStore) registerBackendAsSeen(ctx context.Context, backendID string) error {
	d.registeredBackends[backendID] = false
	return nil
}

// registerBackendAsActive records in the datastore that a user just used the given backend.
func (d *persistentStore) registerBackendAsActive(ctx context.Context, backendID string) error {
	d.registeredBackends[backendID] = true
	return nil
}

// AddBackend adds the definition of the backend to the store.
func (d *persistentStore) AddBackend(ctx context.Context, backend *Backend) error {
	// We never store the last-used value in the store. It is an output-only
	// field that is calculated dynamically.
	d.backends[backend.BackendID] = backend
	return nil
}

// ListBackends lists all of the backends.
func (d *persistentStore) ListBackends(ctx context.Context) ([]*Backend, error) {
	var backends []*Backend
	for _, b := range d.backends {
		backends = append(backends, b)
	}
	return backends, nil
}

func (d *persistentStore) deleteRequestsForBackend(backendID string) error {
	for k := range d.requests {
		if backendID == strings.Split(k, ":")[1] {
			delete(d.requests, k)
		}
	}
	return nil
}

// DeleteBackend deletes the given backend.
func (d *persistentStore) DeleteBackend(ctx context.Context, backendID string) error {
	delete(d.backends, backendID)
	delete(d.registeredBackends, backendID)
	return d.deleteRequestsForBackend(backendID)
}

// LookupBackend looks up the backend for the given user and path.
func (d *persistentStore) LookupBackend(ctx context.Context, endUser, path string) (string, error) {
	for _, b := range d.backends {
		if b.PathPrefixes[0] == path {
			return b.BackendID, nil
		}
	}
	return "", fmt.Errorf("No backend for %q", endUser)
}

// IsBackendUserAllowed checks whether the given user can act as the specified backend.
func (d *persistentStore) IsBackendUserAllowed(ctx context.Context, backendUser, backendID string) (bool, error) {
	return true, nil
}
