/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package jsonld_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/piprate/json-gold/ld"
	"github.com/stretchr/testify/require"

	"github.com/hyperledger/aries-framework-go/pkg/doc/jsonld"
	"github.com/hyperledger/aries-framework-go/pkg/doc/jsonld/context"
	mockstorage "github.com/hyperledger/aries-framework-go/pkg/mock/storage"
	"github.com/hyperledger/aries-framework-go/spi/storage"
)

func TestNewDocumentLoader(t *testing.T) {
	t.Run("Load embed contexts by default", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()

		loader, err := jsonld.NewDocumentLoader(storageProvider)

		require.NotNil(t, loader)
		require.NoError(t, err)
		require.Equal(t, len(context.Embedded), len(storageProvider.Store.Store))
	})

	t.Run("Extra context replaces embedded context with the same URL", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()

		embedded := context.Embedded[0]
		extra := &context.Document{
			URL:     embedded.URL,
			Content: json.RawMessage(`{"@context":"extra"}`),
		}

		loader, err := jsonld.NewDocumentLoader(storageProvider, jsonld.WithExtraContexts(extra))
		require.NoError(t, err)
		require.NotNil(t, loader)
		require.Equal(t, len(context.Embedded), len(storageProvider.Store.Store))

		stored, ok := storageProvider.Store.Store[extra.URL]
		require.True(t, ok)

		var rd ld.RemoteDocument

		err = json.Unmarshal(stored.Value, &rd)
		require.NoError(t, err)

		require.Equal(t, "extra", rd.Document.(map[string]interface{})["@context"])
	})

	t.Run("Load contexts from remote providers", func(t *testing.T) {
		contextsDBStore := &mockStore{
			MockStore: mockstorage.MockStore{
				Store: make(map[string]mockstorage.DBEntry),
			},
		}

		remoteProvidersDBStore := &mockStore{
			MockStore: mockstorage.MockStore{
				Store: make(map[string]mockstorage.DBEntry),
			},
		}

		storageProvider := &mockStoreProvider{
			Stores: map[string]storage.Store{
				jsonld.ContextsDB:        contextsDBStore,
				jsonld.RemoteProvidersDB: remoteProvidersDBStore,
			},
		}

		p1 := &mockRemoteProvider{
			IDValue: "p1",
			ContextsValue: []*context.Document{
				{
					URL:     "https://json-ld.org/contexts/context-1.jsonld",
					Content: json.RawMessage(`{"@context":"context-1"}`),
				},
				{
					URL:     "https://json-ld.org/contexts/context-2.jsonld",
					Content: json.RawMessage(`{"@context":"context-2"}`),
				},
			},
		}

		p2 := &mockRemoteProvider{
			IDValue: "p3",
			ContextsValue: []*context.Document{
				{
					URL:     "https://www.w3.org/2018/credentials/v1",
					Content: json.RawMessage(`{"@context":"updated credentials v1"}`),
				},
			},
		}

		loader, err := jsonld.NewDocumentLoader(storageProvider,
			jsonld.WithRemoteProvider(p1),
			jsonld.WithRemoteProvider(p2),
		)

		require.NotNil(t, loader)
		require.NoError(t, err)

		assertContext(t, contextsDBStore, "https://json-ld.org/contexts/context-1.jsonld", "context-1")
		assertContext(t, contextsDBStore, "https://json-ld.org/contexts/context-2.jsonld", "context-2")
		assertContext(t, contextsDBStore, "https://www.w3.org/2018/credentials/v1", "updated credentials v1")

		loader2, err := jsonld.NewDocumentLoader(storageProvider,
			jsonld.WithRemoteProvider(p1),
			jsonld.WithRemoteProvider(p2),
		)

		require.NotNil(t, loader2)
		require.NoError(t, err)
		require.Equal(t, 2, remoteProvidersDBStore.PutCount)
	})

	t.Run("Fail to open context DB store", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()
		storageProvider.FailNamespace = jsonld.ContextsDB

		loader, err := jsonld.NewDocumentLoader(storageProvider)

		require.Nil(t, loader)
		require.Error(t, err)
	})

	t.Run("Fail to read context document file", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()

		loader, err := jsonld.NewDocumentLoader(storageProvider,
			jsonld.WithExtraContexts(&context.Document{URL: "url", Content: nil}))

		require.Nil(t, loader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "document from reader")
	})

	t.Run("Fail to store batch of context documents", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()
		storageProvider.Store.ErrBatch = errors.New("batch error")

		loader, err := jsonld.NewDocumentLoader(storageProvider)

		require.Nil(t, loader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "store batch of contexts")
	})

	t.Run("Fail to get contexts from the remote provider", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()

		loader, err := jsonld.NewDocumentLoader(storageProvider, jsonld.WithRemoteProvider(
			&mockRemoteProvider{
				ErrContexts: errors.New("error"),
			}),
		)

		require.Nil(t, loader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "get contexts from provider: error")
	})
}

func TestLoadDocument(t *testing.T) {
	t.Run("Load context from store", func(t *testing.T) {
		c, err := ld.DocumentFromReader(strings.NewReader(sampleJSONLDContext))
		require.NotNil(t, c)
		require.NoError(t, err)

		b, err := json.Marshal(&ld.RemoteDocument{
			DocumentURL: "https://example.com/context.jsonld",
			Document:    c,
		})
		require.NoError(t, err)

		storageProvider := mockstorage.NewMockStoreProvider()
		storageProvider.Store.Store = map[string]mockstorage.DBEntry{
			"https://example.com/context.jsonld": {
				Value: b,
			},
		}

		loader, err := jsonld.NewDocumentLoader(storageProvider)
		require.NotNil(t, loader)
		require.NoError(t, err)

		rd, err := loader.LoadDocument("https://example.com/context.jsonld")

		require.NotNil(t, rd)
		require.NoError(t, err)
	})

	t.Run("Fetch remote context document and import into store", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()
		storageProvider.Store.ErrGet = storage.ErrDataNotFound

		loader, err := jsonld.NewDocumentLoader(storageProvider,
			jsonld.WithRemoteDocumentLoader(&mockDocumentLoader{}))

		require.NotNil(t, loader)
		require.NoError(t, err)

		rd, err := loader.LoadDocument("https://example.com/context.jsonld")

		require.NotNil(t, rd)
		require.NoError(t, err)

		require.NotNil(t, storageProvider.Store.Store["https://example.com/context.jsonld"])
	})

	t.Run("ErrContextNotFound if no context document in store", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()
		storageProvider.Store.ErrGet = storage.ErrDataNotFound

		loader, err := jsonld.NewDocumentLoader(storageProvider)
		require.NotNil(t, loader)
		require.NoError(t, err)

		rd, err := loader.LoadDocument("https://example.com/context.jsonld")

		require.Nil(t, rd)
		require.EqualError(t, err, jsonld.ErrContextNotFound.Error())
	})

	t.Run("Fail to get context from store", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()
		storageProvider.Store.ErrGet = errors.New("get error")

		loader, err := jsonld.NewDocumentLoader(storageProvider)
		require.NotNil(t, loader)
		require.NoError(t, err)

		rd, err := loader.LoadDocument("https://example.com/context.jsonld")

		require.Nil(t, rd)
		require.Error(t, err)
	})

	t.Run("Fail to load remote context document", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()
		storageProvider.Store.ErrGet = storage.ErrDataNotFound

		loader, err := jsonld.NewDocumentLoader(storageProvider,
			jsonld.WithRemoteDocumentLoader(&mockDocumentLoader{ErrLoadDocument: errors.New("load document error")}))
		require.NotNil(t, loader)
		require.NoError(t, err)

		rd, err := loader.LoadDocument("https://example.com/context.jsonld")

		require.Nil(t, rd)
		require.Error(t, err)
		require.Contains(t, err.Error(), "load remote context document")
	})

	t.Run("Fail to save fetched remote document", func(t *testing.T) {
		storageProvider := mockstorage.NewMockStoreProvider()
		storageProvider.Store.ErrGet = storage.ErrDataNotFound
		storageProvider.Store.ErrPut = errors.New("put error")

		loader, err := jsonld.NewDocumentLoader(storageProvider,
			jsonld.WithRemoteDocumentLoader(&mockDocumentLoader{}))
		require.NotNil(t, loader)
		require.NoError(t, err)

		rd, err := loader.LoadDocument("https://example.com/context.jsonld")

		require.Nil(t, rd)
		require.Error(t, err)
		require.Contains(t, err.Error(), "save remote document")
	})
}

func assertContext(t *testing.T, store storage.Store, url, value string) {
	t.Helper()

	b, err := store.Get(url)
	require.NoError(t, err)

	var rd ld.RemoteDocument

	err = json.Unmarshal(b, &rd)
	require.NoError(t, err)

	require.Equal(t, value, rd.Document.(map[string]interface{})["@context"])
}

const sampleJSONLDContext = `
{
  "@context": {
    "name": "http://xmlns.com/foaf/0.1/name",
    "homepage": {
      "@id": "http://xmlns.com/foaf/0.1/homepage",
      "@type": "@id"
    }
  }
}`

type mockDocumentLoader struct {
	ErrLoadDocument error
}

func (m *mockDocumentLoader) LoadDocument(string) (*ld.RemoteDocument, error) {
	if m.ErrLoadDocument != nil {
		return nil, m.ErrLoadDocument
	}

	content, err := ld.DocumentFromReader(strings.NewReader(sampleJSONLDContext))
	if err != nil {
		return nil, err
	}

	return &ld.RemoteDocument{
		DocumentURL: "https://example.com/context.jsonld",
		Document:    content,
	}, nil
}

type mockRemoteProvider struct {
	IDValue       string
	EndpointValue string
	ContextsValue []*context.Document
	ErrContexts   error
}

func (p *mockRemoteProvider) ID() string {
	return p.IDValue
}

func (p *mockRemoteProvider) Endpoint() string {
	return p.EndpointValue
}

func (p *mockRemoteProvider) Contexts() ([]*context.Document, error) {
	if p.ErrContexts != nil {
		return nil, p.ErrContexts
	}

	return p.ContextsValue, nil
}

type mockStoreProvider struct {
	Stores map[string]storage.Store
}

// OpenStore opens a Store with the given name and returns a handle.
func (s *mockStoreProvider) OpenStore(name string) (storage.Store, error) {
	return s.Stores[name], nil
}

// SetStoreConfig always return a nil error.
func (s *mockStoreProvider) SetStoreConfig(name string, config storage.StoreConfiguration) error {
	return nil
}

// GetStoreConfig is not implemented.
func (s *mockStoreProvider) GetStoreConfig(name string) (storage.StoreConfiguration, error) {
	panic("implement me")
}

// GetOpenStores is not implemented.
func (s *mockStoreProvider) GetOpenStores() []storage.Store {
	panic("implement me")
}

// Close closes all stores created under this store provider.
func (s *mockStoreProvider) Close() error {
	return nil
}

type mockStore struct {
	mockstorage.MockStore
	PutCount int
}

func (s *mockStore) Put(k string, v []byte, tags ...storage.Tag) error {
	s.PutCount++
	return s.MockStore.Put(k, v, tags...)
}
