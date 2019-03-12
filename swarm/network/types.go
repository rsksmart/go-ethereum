package network

import (
	"fmt"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/protocols"
)

const (
	DefaultNetworkID = 3
)

// BzzPeer is the bzz protocol view of a protocols.Peer (itself an extension of p2p.Peer)
// implements the Peer interface and all interfaces Peer implements: Addr, OverlayPeer
type BzzPeer struct {
	*protocols.Peer           // represents the connection for online peers
	*BzzAddr                  // remote address -> implements Addr interface = protocols.Peer
	lastActive      time.Time // time is updated whenever mutexes are releasing
	LightNode       bool
}

func NewBzzPeer(p *protocols.Peer) *BzzPeer {
	return &BzzPeer{Peer: p, BzzAddr: NewAddr(p.Node())}
}

// ID returns the peer's underlay node identifier.
func (p *BzzPeer) ID() enode.ID {
	// This is here to resolve a method tie: both protocols.Peer and BzzAddr are embedded
	// into the struct and provide ID(). The protocols.Peer version is faster, ensure it
	// gets used.
	return p.Peer.ID()
}

// BzzAddr implements the PeerAddr interface
type BzzAddr struct {
	OAddr []byte
	UAddr []byte
}

// Address implements OverlayPeer interface to be used in Overlay.
func (a *BzzAddr) Address() []byte {
	return a.OAddr
}

// Over returns the overlay address.
func (a *BzzAddr) Over() []byte {
	return a.OAddr
}

// Under returns the underlay address.
func (a *BzzAddr) Under() []byte {
	return a.UAddr
}

// ID returns the node identifier in the underlay.
func (a *BzzAddr) ID() enode.ID {
	n, err := enode.ParseV4(string(a.UAddr))
	if err != nil {
		return enode.ID{}
	}
	return n.ID()
}

// Update updates the underlay address of a peer record
func (a *BzzAddr) Update(na *BzzAddr) *BzzAddr {
	return &BzzAddr{a.OAddr, na.UAddr}
}

// String pretty prints the address
func (a *BzzAddr) String() string {
	return fmt.Sprintf("%x <%s>", a.OAddr, a.UAddr)
}

// RandomAddr is a utility method generating an address from a public key
func RandomAddr() *BzzAddr {
	key, err := crypto.GenerateKey()
	if err != nil {
		panic("unable to generate key")
	}
	node := enode.NewV4(&key.PublicKey, net.IP{127, 0, 0, 1}, 30303, 30303)
	return NewAddr(node)
}

// NewAddr constucts a BzzAddr from a node record.
func NewAddr(node *enode.Node) *BzzAddr {
	//return &BzzAddr{OAddr: node.ID().Bytes(), UAddr: []byte(node.String())}
	return getENRBzzAddr(node)
}
