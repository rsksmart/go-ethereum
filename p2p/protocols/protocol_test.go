// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package protocols

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/rlp"

	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
)

// message to kill/drop the peer with nodeID
type kill struct {
	C enode.ID
}

// message to drop connection
type drop struct {
}

// newProtocol sets up a protocol
// the run function here demonstrates a typical protocol using peerPool, handshake
// and messages registered to handlers
func newProtocol(pp *p2ptest.TestPeerPool) func(*p2p.Peer, p2p.MsgReadWriter) error {
	spec := &Spec{
		Name:       "test",
		Version:    42,
		MaxMsgSize: 10 * 1024,
		Messages: []interface{}{
			kill{},
			drop{},
		},
	}
	return func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
		peer := NewPeer(p, rw, spec)

		handle := func(ctx context.Context, msg interface{}) error {
			switch msg := msg.(type) {

			case *kill:
				// demonstrates use of peerPool, killing another peer connection as a response to a message
				id := msg.C
				pp.Get(id).Drop(errors.New("killed"))
				return nil

			case *drop:
				// for testing we can trigger self induced disconnect upon receiving drop message
				return errors.New("dropped")

			default:
				return fmt.Errorf("unknown message type: %T", msg)
			}
		}

		pp.Add(peer)
		defer pp.Remove(peer)
		return peer.Run(handle)
	}
}

func protocolTester(pp *p2ptest.TestPeerPool) *p2ptest.ProtocolTester {
	conf := adapters.RandomNodeConfig()
	return p2ptest.NewProtocolTester(conf.ID, 2, newProtocol(pp))
}

type dummyHook struct {
	peer  *Peer
	size  uint32
	msg   interface{}
	send  bool
	err   error
	waitC chan struct{}
}

type dummyMsg struct {
	Content string
}

func (d *dummyHook) Send(peer *Peer, size uint32, msg interface{}) error {
	d.peer = peer
	d.size = size
	d.msg = msg
	d.send = true
	return d.err
}

func (d *dummyHook) Receive(peer *Peer, size uint32, msg interface{}) error {
	d.peer = peer
	d.size = size
	d.msg = msg
	d.send = false
	d.waitC <- struct{}{}
	return d.err
}

func TestProtocolHook(t *testing.T) {
	testHook := &dummyHook{
		waitC: make(chan struct{}, 1),
	}
	spec := &Spec{
		Name:       "test",
		Version:    42,
		MaxMsgSize: 10 * 1024,
		Messages: []interface{}{
			dummyMsg{},
		},
		Hook: testHook,
	}

	runFunc := func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
		peer := NewPeer(p, rw, spec)
		ctx := context.TODO()
		err := peer.Send(ctx, &dummyMsg{
			Content: "handshake"})

		if err != nil {
			t.Fatal(err)
		}

		handle := func(ctx context.Context, msg interface{}) error {
			return nil
		}

		return peer.Run(handle)
	}

	conf := adapters.RandomNodeConfig()
	tester := p2ptest.NewProtocolTester(conf.ID, 2, runFunc)
	err := tester.TestExchanges(p2ptest.Exchange{
		Expects: []p2ptest.Expect{
			{
				Code: 0,
				Msg:  &dummyMsg{Content: "handshake"},
				Peer: tester.Nodes[0].ID(),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if testHook.msg == nil || testHook.msg.(*dummyMsg).Content != "handshake" {
		t.Fatal("Expected msg to be set, but it is not")
	}
	if !testHook.send {
		t.Fatal("Expected a send message, but it is not")
	}
	if testHook.peer == nil {
		t.Fatal("Expected peer to be set, is nil")
	}
	if peerId := testHook.peer.ID(); peerId != tester.Nodes[0].ID() && peerId != tester.Nodes[1].ID() {
		t.Fatalf("Expected peer ID to be set correctly, but it is not (got %v, exp %v or %v", peerId, tester.Nodes[0].ID(), tester.Nodes[1].ID())
	}
	if testHook.size != 11 { //11 is the length of the encoded message
		t.Fatalf("Expected size to be %d, but it is %d ", 1, testHook.size)
	}

	err = tester.TestExchanges(p2ptest.Exchange{
		Triggers: []p2ptest.Trigger{
			{
				Code: 0,
				Msg:  &dummyMsg{Content: "response"},
				Peer: tester.Nodes[1].ID(),
			},
		},
	})

	<-testHook.waitC

	if err != nil {
		t.Fatal(err)
	}
	if testHook.msg == nil || testHook.msg.(*dummyMsg).Content != "response" {
		t.Fatal("Expected msg to be set, but it is not")
	}
	if testHook.send {
		t.Fatal("Expected a send message, but it is not")
	}
	if testHook.peer == nil || testHook.peer.ID() != tester.Nodes[1].ID() {
		t.Fatal("Expected peer ID to be set correctly, but it is not")
	}
	if testHook.size != 10 { //11 is the length of the encoded message
		t.Fatalf("Expected size to be %d, but it is %d ", 1, testHook.size)
	}

	testHook.err = fmt.Errorf("dummy error")
	err = tester.TestExchanges(p2ptest.Exchange{
		Triggers: []p2ptest.Trigger{
			{
				Code: 0,
				Msg:  &dummyMsg{Content: "response"},
				Peer: tester.Nodes[1].ID(),
			},
		},
	})

	<-testHook.waitC

	time.Sleep(100 * time.Millisecond)
	err = tester.TestDisconnected(&p2ptest.Disconnect{Peer: tester.Nodes[1].ID(), Error: testHook.err})
	if err != nil {
		t.Fatalf("Expected a specific disconnect error, but got different one: %v", err)
	}

}

//We need to test that if the hook is not defined, then message infrastructure
//(send,receive) still works
func TestNoHook(t *testing.T) {
	//create a test spec
	spec := createTestSpec()
	//a random node
	id := adapters.RandomNodeConfig().ID
	//a peer
	p := p2p.NewPeer(id, "testPeer", nil)
	rw := &dummyRW{}
	peer := NewPeer(p, rw, spec)
	ctx := context.TODO()
	msg := &perBytesMsgSenderPays{Content: "testBalance"}
	//send a message
	err := peer.Send(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	//simulate receiving a message
	rw.msg = msg
	peer.handleIncoming(func(ctx context.Context, msg interface{}) error {
		return nil
	})
	//all should just work and not result in any error
}

func runMultiplePeers(t *testing.T, peer int, errs ...error) {

	pp := p2ptest.NewTestPeerPool()
	s := protocolTester(pp)

	// peer 0 sends kill request for peer with index <peer>
	err := s.TestExchanges(p2ptest.Exchange{
		Triggers: []p2ptest.Trigger{
			{
				Code: 2,
				Msg:  &kill{s.Nodes[peer].ID()},
				Peer: s.Nodes[0].ID(),
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	// the peer not killed sends a drop request
	err = s.TestExchanges(p2ptest.Exchange{
		Triggers: []p2ptest.Trigger{
			{
				Code: 3,
				Msg:  &drop{},
				Peer: s.Nodes[(peer+1)%2].ID(),
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	// check the actual discconnect errors on the individual peers
	var disconnects []*p2ptest.Disconnect
	for i, err := range errs {
		disconnects = append(disconnects, &p2ptest.Disconnect{Peer: s.Nodes[i].ID(), Error: err})
	}
	if err := s.TestDisconnected(disconnects...); err != nil {
		t.Fatal(err)
	}
	// test if disconnected peers have been removed from peerPool
	if pp.Has(s.Nodes[peer].ID()) {
		t.Fatalf("peer test-%v not dropped: %v (%v)", peer, pp, s.Nodes)
	}

}
func XTestMultiplePeersDropSelf(t *testing.T) {
	runMultiplePeers(t, 0,
		fmt.Errorf("subprotocol error"),
		fmt.Errorf("Message handler error: (msg code 3): dropped"),
	)
}

func XTestMultiplePeersDropOther(t *testing.T) {
	runMultiplePeers(t, 1,
		fmt.Errorf("Message handler error: (msg code 3): dropped"),
		fmt.Errorf("subprotocol error"),
	)
}

//dummy implementation of a MsgReadWriter
//this allows for quick and easy unit tests without
//having to build up the complete protocol
type dummyRW struct {
	msg  interface{}
	size uint32
	code uint64
}

func (d *dummyRW) WriteMsg(msg p2p.Msg) error {
	return nil
}

func (d *dummyRW) ReadMsg() (p2p.Msg, error) {
	enc := bytes.NewReader(d.getDummyMsg())
	return p2p.Msg{
		Code:       d.code,
		Size:       d.size,
		Payload:    enc,
		ReceivedAt: time.Now(),
	}, nil
}

func (d *dummyRW) getDummyMsg() []byte {
	r, _ := rlp.EncodeToBytes(d.msg)
	var b bytes.Buffer
	wmsg := WrappedMsg{
		Context: b.Bytes(),
		Size:    uint32(len(r)),
		Payload: r,
	}
	rr, _ := rlp.EncodeToBytes(wmsg)
	d.size = uint32(len(rr))
	return rr
}
