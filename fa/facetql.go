package fa

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// FacetQLClient is the native Facet runtime client for FacetQL.
//
// Facet applications should talk to FacetQL through this client rather
// than constructing HTTP requests themselves.
type FacetQLClient struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// NewFacetQL creates a FacetQL client.
//
// Example:
//
//	db := fa.NewFacetQL("http://localhost:8080", os.Getenv("FACETQL_TOKEN"))
func NewFacetQL(baseURL, apiKey string) *FacetQLClient {
	return &FacetQLClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey: apiKey,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// FacetQLNode is the storage representation used by the runtime.
//
// The data field intentionally remains JSON text because FacetQL owns
// the underlying node representation.
type FacetQLNode struct {
	Address   string `json:"address"`
	Kind      string `json:"kind"`
	X         uint8  `json:"x"`
	Y         uint8  `json:"y"`
	Z         uint8  `json:"z"`
	Q         uint8  `json:"q"`
	Data      string `json:"data"`
	Public    bool   `json:"public"`
	Owner     string `json:"owner,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// Put stores or replaces a node in FacetQL.
func (db *FacetQLClient) Put(
	ctx context.Context,
	node FacetQLNode,
) error {
	body, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("facetql marshal node: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		db.BaseURL+"/node",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("facetql create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	if db.APIKey != "" {
		req.Header.Set("x-api-key", db.APIKey)
	}

	resp, err := db.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("facetql write: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf(
			"facetql write failed: HTTP %s",
			resp.Status,
		)
	}

	return nil
}

// Get retrieves a node from FacetQL.
func (db *FacetQLClient) Get(
	ctx context.Context,
	address string,
) (*FacetQLNode, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		db.BaseURL+"/node/"+address,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("facetql create request: %w", err)
	}

	if db.APIKey != "" {
		req.Header.Set("x-api-key", db.APIKey)
	}

	resp, err := db.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("facetql read: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf(
			"facetql read failed: HTTP %s",
			resp.Status,
		)
	}

	var node FacetQLNode

	if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
		return nil, fmt.Errorf(
			"facetql decode node: %w",
			err,
		)
	}

	return &node, nil
}

// Delete removes a node from FacetQL.
func (db *FacetQLClient) Delete(
	ctx context.Context,
	address string,
) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		db.BaseURL+"/node/"+address,
		nil,
	)
	if err != nil {
		return fmt.Errorf("facetql create request: %w", err)
	}

	if db.APIKey != "" {
		req.Header.Set("x-api-key", db.APIKey)
	}

	resp, err := db.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("facetql delete: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf(
			"facetql delete failed: HTTP %s",
			resp.Status,
		)
	}

	return nil
}