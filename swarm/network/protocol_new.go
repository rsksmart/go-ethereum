package network

import (
	"context"
	"errors"
	"sync"

	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/swarm/log"
	"github.com/ethereum/go-ethereum/swarm/state"
)

type BzzTwo struct {
	*Hive
	NetworkID    uint64
	LightNode    bool
	BootNodeMode bool
	streamerSpec *protocols.Spec
	streamerRun  func(*BzzPeer) error
	peerInit     map[enode.ID]chan *BzzPeer
	enr          []enr.Entry
	mu           sync.Mutex
}

func NewBzzTwo(config *BzzConfig, kad *Kademlia, store state.Store, streamerSpec *protocols.Spec, streamerRun func(*BzzPeer) error) *BzzTwo {

	var lightnodeentry ENRLightNodeEntry
	var bootnodeentry ENRBootNodeEntry

	bzz := &BzzTwo{
		Hive:      NewHive(config.HiveParams, kad, store),
		NetworkID: config.NetworkID,
		enr: []enr.Entry{
			*NewENRAddrEntry(config.OverlayAddr),
			lightnodeentry,
			bootnodeentry,
		},
		streamerRun:  streamerRun,
		streamerSpec: streamerSpec,
		peerInit:     make(map[enode.ID]chan *BzzPeer),
	}
	return bzz
}

func (b *BzzTwo) NodeInfo() interface{} {
	return b.BaseAddr()
}

// wait until initialzation of peer is completed
// currently, it only means that the bzzkey entry is successfully inserted in the kademlia
func (b *BzzTwo) getOrCreatePeerInit(id enode.ID) chan *BzzPeer {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.peerInit[id]; !ok {
		b.peerInit[id] = make(chan *BzzPeer)
	}
	return b.peerInit[id]
}

var BzzTwoSpec = &protocols.Spec{
	Name:       "bzz",
	Version:    9,
	MaxMsgSize: 0,
}

func (b *BzzTwo) Protocols() []p2p.Protocol {
	protocol := []p2p.Protocol{
		{
			Name:     DiscoverySpec.Name,
			Version:  DiscoverySpec.Version,
			Length:   DiscoverySpec.Length(),
			Run:      b.RunProtocol(DiscoverySpec, b.RunHiveTmp),
			NodeInfo: b.Hive.NodeInfo,
			PeerInfo: b.Hive.PeerInfo,
		},
		{
			Name:       BzzTwoSpec.Name,
			Version:    BzzTwoSpec.Version,
			Length:     BzzTwoSpec.Length(),
			Run:        b.RunProtocol(BzzTwoSpec, b.runBzz),
			NodeInfo:   b.NodeInfo,
			Attributes: b.enr,
		},
	}
	if b.streamerSpec != nil && b.streamerRun != nil {
		protocol = append(protocol, p2p.Protocol{
			Name:    b.streamerSpec.Name,
			Version: b.streamerSpec.Version,
			Length:  b.streamerSpec.Length(),
			Run:     b.RunProtocol(b.streamerSpec, b.streamerRun),
		})
	}

	return protocol
}

func (b *BzzTwo) runBzz(bzzPeer *BzzPeer) error {
	return bzzPeer.Run(func(ctx context.Context, msg interface{}) error {
		return nil
	})
}

func (b *BzzTwo) runBzzTest(bzzPeer *BzzPeer) error {
	dp := NewPeer(bzzPeer, b.Kademlia)
	b.On(dp)
	b.getOrCreatePeerInit(bzzPeer.ID()) <- bzzPeer
	return bzzPeer.Run(func(ctx context.Context, msg interface{}) error {
		return nil
	})
}

func (b *BzzTwo) RunProtocol(spec *protocols.Spec, run func(*BzzPeer) error) func(*p2p.Peer, p2p.MsgReadWriter) error {
	return func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
		// TODO this is a temporary solution until the "changed" return from kademlia is implemented as channel. When this is done, the kademlia On call can be completed here, and the channel wait in RunProtocol for the bzzpeer create does not need a contingency
		var bzzPeer *BzzPeer
		//if spec.Name == "bzz" {
		if spec.Name == "hive" {
			bzzPeer = getENRBzzPeer(p, rw, spec)
		} else {
			bzzPeer = <-b.getOrCreatePeerInit(p.ID())
		}
		if bzzPeer == nil {
			return errors.New("nil bzzpeer received")
		}
		log.Info("bzz protocol started", "name", spec.Name)
		return run(bzzPeer)
	}
}

// APIs returns the APIs offered by bzz
// * hive
// Bzz implements the node.Service interface
func (b *BzzTwo) APIs() []rpc.API {
	return []rpc.API{{
		Namespace: "hive",
		Version:   "3.0",
		Service:   b.Hive,
	}}
}

// Run protocol run function
// TODO temporary elaboration of Hive.Run until we can separate out the kademlia.On call
func (b *BzzTwo) RunHiveTmp(p *BzzPeer) error {
	b.trackPeer(p)
	defer b.untrackPeer(p)

	dp := NewPeer(p, b.Kademlia)
	depth, changed := b.On(dp)
	b.getOrCreatePeerInit(p.ID()) <- p

	// if we want discovery, advertise change of depth
	if b.Discovery {
		if changed {
			// if depth changed, send to all peers
			NotifyDepth(depth, b.Kademlia)
		} else {
			// otherwise just send depth to new peer
			dp.NotifyDepth(depth)
		}
		NotifyPeer(p.BzzAddr, b.Kademlia)
	}
	defer b.Off(dp)
	return dp.Run(dp.HandleMsg)
}
