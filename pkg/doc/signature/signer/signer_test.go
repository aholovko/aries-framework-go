/*
Copyright SecureKey Technologies Inc. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package signer

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hyperledger/aries-framework-go/pkg/doc/signature/proof"
	"github.com/hyperledger/aries-framework-go/pkg/doc/signature/suite"
	"github.com/hyperledger/aries-framework-go/pkg/doc/signature/suite/ed25519signature2018"
)

const signatureType = "Ed25519Signature2018"

func TestDocumentSigner_Sign(t *testing.T) {
	context := getSignatureContext()

	s := New(ed25519signature2018.New(
		suite.WithSigner(
			getSigner(generatePrivateKey()))))
	signedDoc, err := s.Sign(context, []byte(validDoc))
	require.NoError(t, err)
	require.NotNil(t, signedDoc)

	context.SignatureRepresentation = proof.SignatureJWS
	signedJWSDoc, err := s.Sign(context, []byte(validDoc))
	require.NoError(t, err)
	require.NotNil(t, signedJWSDoc)
}

func TestDocumentSigner_SignErrors(t *testing.T) {
	context := getSignatureContext()
	s := New(ed25519signature2018.New(
		suite.WithSigner(
			getSigner(generatePrivateKey()))))

	// test invalid json
	signedDoc, err := s.Sign(context, []byte("not json"))
	require.NotNil(t, err)
	require.Nil(t, signedDoc)
	require.Contains(t, err.Error(), "failed to unmarshal json ld document")

	// test for signature suite not supported
	context = getSignatureContext()
	context.SignatureType = "non-existent"
	signedDoc, err = s.Sign(context, []byte(validDoc))
	require.NotNil(t, err)
	require.Nil(t, signedDoc)
	require.Contains(t, err.Error(), "signature type non-existent not supported")

	// test verify data creation error
	var validDocMap map[string]interface{}

	err = json.Unmarshal([]byte(validDoc), &validDocMap)
	require.NoError(t, err)

	validDocMap["@context"] = "invalid context"
	invalidDocBytes, err := json.Marshal(validDocMap)
	require.NoError(t, err)

	context = getSignatureContext()
	signedDoc, err = s.Sign(context, invalidDocBytes)
	require.NotNil(t, err)
	require.Nil(t, signedDoc)
	require.Contains(t, err.Error(), "invalid context")

	// test signing error
	context = getSignatureContext()
	s = New(ed25519signature2018.New(
		suite.WithSigner(
			getSigner([]byte("invalid")))))
	signedDoc, err = s.Sign(context, []byte(validDoc))
	require.NotNil(t, err)
	require.Nil(t, signedDoc)
	require.Contains(t, err.Error(), "bad private key length")
}

func TestDocumentSigner_isValidContext(t *testing.T) {
	s := New()

	context := getSignatureContext()
	context.SignatureType = ""
	signedDoc, err := s.Sign(context, []byte(validDoc))
	require.NotNil(t, err)
	require.Nil(t, signedDoc)
	require.Contains(t, err.Error(), "signature type is missing")
}

func getSignatureContext() *Context {
	return &Context{Creator: "creator",
		SignatureType: signatureType}
}

func generatePrivateKey() []byte {
	_, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}

	return privKey
}

func getSigner(privKey []byte) *testSigner {
	return &testSigner{privateKey: privKey}
}

type testSigner struct {
	privateKey []byte
}

func (s *testSigner) Sign(doc []byte) ([]byte, error) {
	if l := len(s.privateKey); l != ed25519.PrivateKeySize {
		return nil, errors.New("ed25519: bad private key length")
	}

	return ed25519.Sign(s.privateKey, doc), nil
}

//nolint:lll
const validDoc = `{
  "@context": ["https://w3id.org/did/v1"],
  "id": "did:example:21tDAKCERh95uGgKbJNHYp",
  "publicKey": [
    {
      "id": "did:example:123456789abcdefghi#keys-1",
      "type": "Secp256k1VerificationKey2018",
      "controller": "did:example:123456789abcdefghi",
      "publicKeyBase58": "H3C2AVvLMv6gmMNam3uVAjZpfkcJCwDwnZn6z3wXmqPV"
    },
    {
      "id": "did:example:123456789abcdefghw#key2",
      "type": "RsaVerificationKey2018",
      "controller": "did:example:123456789abcdefghw",
      "publicKeyPem": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAryQICCl6NZ5gDKrnSztO\n3Hy8PEUcuyvg/ikC+VcIo2SFFSf18a3IMYldIugqqqZCs4/4uVW3sbdLs/6PfgdX\n7O9D22ZiFWHPYA2k2N744MNiCD1UE+tJyllUhSblK48bn+v1oZHCM0nYQ2NqUkvS\nj+hwUU3RiWl7x3D2s9wSdNt7XUtW05a/FXehsPSiJfKvHJJnGOX0BgTvkLnkAOTd\nOrUZ/wK69Dzu4IvrN4vs9Nes8vbwPa/ddZEzGR0cQMt0JBkhk9kU/qwqUseP1QRJ\n5I1jR4g8aYPL/ke9K35PxZWuDp3U0UPAZ3PjFAh+5T+fc7gzCs9dPzSHloruU+gl\nFQIDAQAB\n-----END PUBLIC KEY-----"
    }
  ],
  "created": "2002-10-10T17:00:00Z"
}`
