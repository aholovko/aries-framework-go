/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package jsonld

//nolint:gosec
import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/piprate/json-gold/ld"

	"github.com/hyperledger/aries-framework-go/pkg/common/log"
	"github.com/hyperledger/aries-framework-go/pkg/doc/jsonld/context"
	"github.com/hyperledger/aries-framework-go/spi/storage"
)

const (
	// ContextsDB is a name of DB for storing JSON-LD contexts.
	ContextsDB = "jsonldContexts"
	// RemoteProvidersDB is a name of DB with metadata for remote JSON-LD context providers.
	RemoteProvidersDB = "jsonldRemoteProviders"
	// EntryTag is a tag associated with every record in both ContextsDB and RemoteProvidersDB.
	EntryTag = "entry"
)

var logger = log.New("aries-framework/jsonld")

// ErrContextNotFound is returned when JSON-LD context document is not found in the underlying storage.
var ErrContextNotFound = errors.New("context document not found")

// DocumentLoader is an implementation of ld.DocumentLoader backed by storage.
type DocumentLoader struct {
	store                storage.Store
	remoteDocumentLoader ld.DocumentLoader
}

// NewDocumentLoader returns a new DocumentLoader instance.
//
// Embedded contexts (`contexts/third_party`) are always preloaded into the underlying storage.
// Additional contexts can be set using WithExtraContexts() option.
//
// By default, missing contexts are not fetched from the remote URL. Use WithRemoteDocumentLoader() option
// to specify a custom loader that can resolve context documents from the network.
func NewDocumentLoader(storageProvider storage.Provider, opts ...DocumentLoaderOpts) (*DocumentLoader, error) {
	options := &documentLoaderOpts{}

	for i := range opts {
		opts[i](options)
	}

	store, err := storageProvider.OpenStore(ContextsDB)
	if err != nil {
		return nil, fmt.Errorf("open ContextsDB store: %w", err)
	}

	contexts := make(map[string]*context.Document)

	// embedded/extra contexts can be replaced with contexts that come from remote providers
	for _, c := range append(context.Embedded, options.extraContexts...) {
		contexts[c.URL] = c
	}

	// filter out contexts that already exist in DB
	if err = filterOutExistingContexts(store, contexts); err != nil {
		return nil, fmt.Errorf("filter out existing contexts: %w", err)
	}

	// get new/outdated contexts from the remote providers
	remoteContexts, err := getRemoteContexts(storageProvider, options.remoteProviders)
	if err != nil {
		return nil, fmt.Errorf("fetch remote contexts: %w", err)
	}

	for _, c := range remoteContexts {
		contexts[c.URL] = c
	}

	// preload context documents into the underlying storage
	if err = save(store, contexts); err != nil {
		return nil, fmt.Errorf("save context documents: %w", err)
	}

	return &DocumentLoader{
		store:                store,
		remoteDocumentLoader: options.remoteDocumentLoader,
	}, nil
}

//nolint:gocyclo
func getRemoteContexts(sp storage.Provider, providers []RemoteProvider) (map[string]*context.Document, error) {
	store, err := sp.OpenStore(RemoteProvidersDB)
	if err != nil {
		return nil, fmt.Errorf("open RemoteProvidersDB store: %w", err)
	}

	providersInDB, err := getProvidersFromDB(store)
	if err != nil {
		return nil, fmt.Errorf("get remote contexts: %w", err)
	}

	contexts := make(map[string]*context.Document)

	for _, p := range providers {
		cc, err := p.Contexts()
		if err != nil {
			return nil, fmt.Errorf("get contexts from provider: %w", err)
		}

		outdated := false

		pr, found := providersInDB[p.ID()]
		if !found {
			pr = &providerEntry{
				Endpoint:      p.Endpoint(),
				ContentHashes: make(map[string]string),
			}

			outdated = true
		}

		for _, c := range cc {
			hash := md5Hash(c.Content)
			hashInDB, ok := pr.ContentHashes[c.URL]

			if hash != hashInDB || !ok {
				contexts[c.URL] = c
				pr.ContentHashes[c.URL] = hash

				outdated = true
			}
		}

		if !outdated {
			continue
		}

		b, err := json.Marshal(&pr)
		if err != nil {
			return nil, fmt.Errorf("marshal provider entry: %w", err)
		}

		if err = store.Put(p.ID(), b, storage.Tag{Name: EntryTag}); err != nil {
			return nil, fmt.Errorf("put provider entry: %w", err)
		}
	}

	return contexts, nil
}

func md5Hash(data []byte) string {
	hash := md5.Sum(data) //nolint:gosec
	return hex.EncodeToString(hash[:])
}

func filterOutExistingContexts(store storage.Store, contexts map[string]*context.Document) error {
	return forEachEntry(store, func(key string, _ []byte) error {
		delete(contexts, key)
		return nil
	})
}

func getProvidersFromDB(store storage.Store) (map[string]*providerEntry, error) {
	providers := make(map[string]*providerEntry)

	err := forEachEntry(store, func(key string, value []byte) error {
		var entry providerEntry

		err := json.Unmarshal(value, &entry)
		if err != nil {
			return fmt.Errorf("unmarshal value: %w", err)
		}

		providers[key] = &entry
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("get providers from DB: %w", err)
	}

	return providers, nil
}

func forEachEntry(store storage.Store, fn func(key string, value []byte) error) error {
	iter, err := store.Query(EntryTag)
	if err != nil {
		return fmt.Errorf("query store: %w", err)
	}

	defer func() {
		er := iter.Close()
		if er != nil {
			logger.Warnf("failed to close iterator: %s", er.Error())
		}
	}()

	for {
		if ok, err := iter.Next(); !ok || err != nil {
			if err != nil {
				return fmt.Errorf("next entry: %w", err)
			}

			break
		}

		k, err := iter.Key()
		if err != nil {
			return fmt.Errorf("get key: %w", err)
		}

		v, err := iter.Value()
		if err != nil {
			return fmt.Errorf("get value: %w", err)
		}

		err = fn(k, v)
		if err != nil {
			return fmt.Errorf("exec fn: %w", err)
		}
	}

	return nil
}

// save stores contexts into the underlying storage.
func save(store storage.Store, docs map[string]*context.Document) error {
	var ops []storage.Operation

	for _, doc := range docs {
		content, err := ld.DocumentFromReader(bytes.NewReader(doc.Content))
		if err != nil {
			return fmt.Errorf("document from reader: %w", err)
		}

		rd := ld.RemoteDocument{
			DocumentURL: doc.DocumentURL,
			Document:    content,
		}

		b, err := json.Marshal(rd)
		if err != nil {
			return fmt.Errorf("marshal remote document: %w", err)
		}

		ops = append(ops, storage.Operation{
			Key:   doc.URL,
			Value: b,
			Tags:  []storage.Tag{{Name: EntryTag}},
		})
	}

	if len(ops) == 0 {
		return nil
	}

	if err := store.Batch(ops); err != nil {
		return fmt.Errorf("store batch of contexts: %w", err)
	}

	return nil
}

// LoadDocument resolves JSON-LD context document by document URL (u) either from storage or from remote URL.
// If document is not found in the storage and remote DocumentLoader is not specified, ErrContextNotFound is returned.
func (l *DocumentLoader) LoadDocument(u string) (*ld.RemoteDocument, error) {
	b, err := l.store.Get(u)
	if err != nil {
		if !errors.Is(err, storage.ErrDataNotFound) {
			return nil, fmt.Errorf("get context from store: %w", err)
		}

		if l.remoteDocumentLoader == nil { // fetching from the remote URL is disabled
			return nil, ErrContextNotFound
		}

		return l.loadDocumentFromURL(u)
	}

	var rd ld.RemoteDocument

	if err := json.Unmarshal(b, &rd); err != nil {
		return nil, fmt.Errorf("unmarshal context document: %w", err)
	}

	return &rd, nil
}

func (l *DocumentLoader) loadDocumentFromURL(u string) (*ld.RemoteDocument, error) {
	rd, err := l.remoteDocumentLoader.LoadDocument(u)
	if err != nil {
		return nil, fmt.Errorf("load remote context document: %w", err)
	}

	b, err := json.Marshal(rd)
	if err != nil {
		return nil, fmt.Errorf("marshal remote document: %w", err)
	}

	if err := l.store.Put(u, b); err != nil {
		return nil, fmt.Errorf("save remote document: %w", err)
	}

	return rd, nil
}

type documentLoaderOpts struct {
	remoteDocumentLoader ld.DocumentLoader
	extraContexts        []*context.Document
	remoteProviders      []RemoteProvider
}

// DocumentLoaderOpts configures DocumentLoader during creation.
type DocumentLoaderOpts func(opts *documentLoaderOpts)

// WithExtraContexts sets the extra contexts for preloading into the underlying storage.
func WithExtraContexts(contexts ...*context.Document) DocumentLoaderOpts {
	return func(opts *documentLoaderOpts) {
		opts.extraContexts = contexts
	}
}

// WithRemoteDocumentLoader specifies loader for fetching JSON-LD context documents from remote URLs.
// Documents are fetched with this loader only if they are not found in the underlying storage.
func WithRemoteDocumentLoader(loader ld.DocumentLoader) DocumentLoaderOpts {
	return func(opts *documentLoaderOpts) {
		opts.remoteDocumentLoader = loader
	}
}

// RemoteProvider defines a remote JSON-LD context provider.
type RemoteProvider interface {
	ID() string
	Endpoint() string
	Contexts() ([]*context.Document, error)
}

// WithRemoteProvider adds a remote JSON-LD context provider to the list of remote providers.
func WithRemoteProvider(provider RemoteProvider) DocumentLoaderOpts {
	return func(opts *documentLoaderOpts) {
		opts.remoteProviders = append(opts.remoteProviders, provider)
	}
}

// providerEntry represents an entry in DB with metadata for the remote JSON-LD context provider.
type providerEntry struct {
	Endpoint      string            `json:"endpoint"`
	ContentHashes map[string]string `json:"content_hashes"`
}
