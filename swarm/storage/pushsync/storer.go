// Copyright 2019 The go-ethereum Authors
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

package pushsync

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/swarm/chunk"
	"github.com/ethereum/go-ethereum/swarm/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

// Store is the storage interface to save chunks
// NetStore implements this interface
type Store interface {
	Put(context.Context, chunk.ModePut, chunk.Chunk) (bool, error)
}

// Storer is the object used by the push-sync server side protocol
type Storer struct {
	store       Store             // store to put chunks in, and retrieve them
	ps          PubSub            // pubsub interface to receive chunks and send receipts
	deregister  func()            // deregister the registered handler when Storer is closed
	pushReceipt func(addr []byte) // to be called...
}

// NewStorer constructs a Storer
// Storer run storer nodes to handle the reception of push-synced chunks
// that fall within their area of responsibility.
// The protocol makes sure that
// - the chunks are stored and synced to their nearest neighbours and
// - a statement of custody receipt is sent as a response to the originator
// it sets a cancel function that deregisters the handler
func NewStorer(store Store, ps PubSub, pushReceipt func(addr []byte)) *Storer {
	s := &Storer{
		store:       store,
		ps:          ps,
		pushReceipt: pushReceipt,
	}
	s.deregister = ps.Register(pssChunkTopic, true, func(msg []byte, _ *p2p.Peer) error {
		return s.handleChunkMsg(msg)
	})
	return s
}

// Close needs to be called to deregister the handler
func (s *Storer) Close() {
	s.deregister()
}

// handleChunkMsg is called by the pss dispatcher on pssChunkTopic msgs
// - deserialises chunkMsg and
// - calls storer.processChunkMsg function
func (s *Storer) handleChunkMsg(msg []byte) error {
	chmsg, err := decodeChunkMsg(msg)
	if err != nil {
		return err
	}
	log.Debug("Handler", "chunk", label(chmsg.Addr), "origin", label(chmsg.Origin), "self", s.ps.BaseAddr())
	return s.processChunkMsg(chmsg)
}

// processChunkMsg processes a chunk received via pss pssChunkTopic
// these chunk messages are sent to their address as destination
// using neighbourhood addressing. Therefore nodes only handle
// chunks that fall within their area of responsibility.
// Upon receiving the chunk is saved and a statement of custody
// receipt message is sent as a response to the originator.
func (s *Storer) processChunkMsg(chmsg *chunkMsg) error {
	// TODO: double check if it falls in area of responsibility
	ch := storage.NewChunk(chmsg.Addr, chmsg.Data)
	if _, err := s.store.Put(context.TODO(), chunk.ModePutSync, ch); err != nil {
		return err
	}
	// TODO: check if originator or relayer is a nearest neighbour then return
	// otherwise send back receipt
	return s.sendReceiptMsg(chmsg)
}

// sendReceiptMsg sends a statement of custody receipt message
// to the originator of a push-synced chunk message.
// Including a unique nonce makes the receipt immune to deduplication cache
func (s *Storer) sendReceiptMsg(chmsg *chunkMsg) error {
	// if origin is self, use direct channel, no pubsub send needed
	if bytes.Equal(chmsg.Origin, s.ps.BaseAddr()) {
		go s.pushReceipt(chmsg.Addr)
		return nil
	}
	rmsg := &receiptMsg{
		Addr:  chmsg.Addr,
		Nonce: newNonce(),
	}
	msg, err := rlp.EncodeToBytes(rmsg)
	if err != nil {
		return err
	}
	to := chmsg.Origin
	log.Debug("send receipt", "addr", label(rmsg.Addr), "to", label(to), "self", hex.EncodeToString(s.ps.BaseAddr()))
	return s.ps.Send(to, pssReceiptTopic, msg)
}
