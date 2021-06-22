/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package context

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/hyperledger/aries-framework-go/pkg/common/log"
)

const defaultTimeout = time.Minute

var logger = log.New("aries-framework/context/remote")

type options struct {
	httpClient HTTPClient
}

// ProviderOpt configures remote context provider.
type ProviderOpt func(*options)

// HTTPClient represents an HTTP client.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// WithHTTPClient configures an HTTP client.
func WithHTTPClient(client HTTPClient) ProviderOpt {
	return func(opt *options) {
		opt.httpClient = client
	}
}

// RemoteProvider is a remote JSON-LD context provider.
type RemoteProvider struct {
	id       string
	endpoint string
	http     HTTPClient
}

// NewRemoteProvider returns a new instance of the remote provider.
func NewRemoteProvider(id, endpoint string, opts ...ProviderOpt) (*RemoteProvider, error) {
	providerOpts := &options{httpClient: &http.Client{
		Timeout: defaultTimeout,
	}}

	for _, opt := range opts {
		opt(providerOpts)
	}

	return &RemoteProvider{
		id:       id,
		endpoint: endpoint,
		http:     providerOpts.httpClient,
	}, nil
}

// Response represents a response with JSON-LD contexts from the remote source.
type Response struct {
	Documents []*Document `json:"documents"`
}

// ID returns ID of the remote provider.
func (p *RemoteProvider) ID() string {
	return p.id
}

// Endpoint returns endpoint of the remote source.
func (p *RemoteProvider) Endpoint() string {
	return p.endpoint
}

// Contexts returns JSON-LD contexts from the remote source.
func (p *RemoteProvider) Contexts() ([]*Document, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, p.endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}

	resp, err := p.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do: %w", err)
	}

	defer func() {
		e := resp.Body.Close()
		if e != nil {
			logger.Errorf("Failed to close response body: %s", e.Error())
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("response status code: %d", resp.StatusCode)
	}

	var response Response

	if err = json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return response.Documents, nil
}
