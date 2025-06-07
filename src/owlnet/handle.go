package owlnet

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/subtle"
	"encoding/json"
	"errors"

	"golang.org/x/crypto/sha3"
)

// Handle is a reference to a resource on a node
type Handle [32]byte

func (h Handle) MarhsalJSON() ([]byte, error) {
	return json.Marshal(h[:])
}

func (h *Handle) UnmarhsalJSON(data []byte) error {
	var x []byte
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	if h == nil {
		*h = Handle{}
	}
	copy(h[:], x)
	return nil
}

// NewHandle creates a new handle for peerID containing x
func NewHandle(secret *[32]byte, peerID PeerID, data *[16]byte) (out Handle) {
	ciph1, ciph2 := makeCiphers(secret, peerID)
	ciph1.Encrypt(out[:16], data[:])
	ciph2.Encrypt(out[16:], data[:])
	return out
}

// OpenHandle returns the data from the handle or an error if the handle is invalid
func OpenHandle(secret *[32]byte, peerID PeerID, x Handle) (data [16]byte, _ error) {
	ciph1, ciph2 := makeCiphers(secret, peerID)
	ciph1.Decrypt(x[:16], x[:16])
	ciph2.Decrypt(x[16:], x[16:])
	eq := subtle.ConstantTimeCompare(x[:16], x[16:])
	if eq != 1 {
		return [16]byte{}, errors.New("invalid handle")
	}
	copy(data[:], x[:16])
	return data, nil
}

func makeCiphers(secret *[32]byte, peerID PeerID) (left, right cipher.Block) {
	key1, key2 := deriveKeys(secret, peerID)
	ciph1, err := aes.NewCipher(key1[:])
	if err != nil {
		panic(err)
	}
	ciph2, err := aes.NewCipher(key2[:])
	if err != nil {
		panic(err)
	}
	return ciph1, ciph2
}

func deriveKeys(secret *[32]byte, peerID PeerID) (key1 [32]byte, key2 [32]byte) {
	// derive key for peer
	h := sha3.NewShake256()
	h.Write(secret[:])
	h.Write(peerID[:])
	h1, h2 := h, h.Clone()

	h1.Write([]byte("0"))
	h2.Write([]byte("1"))
	h1.Read(key1[:])
	h2.Read(key2[:])
	return key1, key2
}
