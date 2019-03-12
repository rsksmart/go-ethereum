package simulation_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethereum/go-ethereum/swarm/log"
	"github.com/ethereum/go-ethereum/swarm/network"
	"github.com/ethereum/go-ethereum/swarm/network/simulation"
)

func TestENR(t *testing.T) {
	sim := simulation.New(map[string]simulation.ServiceFunc{
		"bzz": func(ctx *adapters.ServiceContext, b *sync.Map) (node.Service, func(), error) {
			addr := network.NewAddr(ctx.Config.Node())
			hp := network.NewHiveParams()
			hp.Discovery = true
			config := &network.BzzConfig{
				OverlayAddr:  addr.Over(),
				UnderlayAddr: addr.Under(),
				HiveParams:   hp,
			}
			log.Warn("addr", "config", config)
			kad := network.NewKademlia(addr.Over(), network.NewKadParams())
			// store kademlia in node's bucket under BucketKeyKademlia
			// so that it can be found by WaitTillHealthy method.
			b.Store(simulation.BucketKeyKademlia, kad)
			return network.NewBzzTwo(config, kad, nil, nil, nil), nil, nil
		},
	})
	defer sim.Close()

	_, err := sim.AddNodesAndConnectRing(10)
	if err != nil {
		// handle error properly...
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ill, err := sim.WaitTillHealthy(ctx)
	if err != nil {
		// inspect the latest detected not healthy kademlias
		for id, kad := range ill {
			fmt.Println("Node", id)
			fmt.Println(kad.String())
		}
		// handle error...
	}
}
