package mempool

import (
	"testing"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/abci/example/kvstore"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/mock"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
)

type outboundConnectionsObserver struct {
	metrics.Gauge
	changes chan float64
}

func (g outboundConnectionsObserver) Add(delta float64) {
	g.Gauge.Add(delta)
	g.changes <- delta
}

func TestReactorDisconnectWhileSyncing(t *testing.T) {
	config := cfg.TestConfig()
	mp, cleanup := newMempoolWithApp(proxy.NewLocalClientCreator(kvstore.NewInMemoryApplication()))
	t.Cleanup(cleanup)
	changes := make(chan float64, 2)
	mp.metrics.ActiveOutboundConnections = outboundConnectionsObserver{
		Gauge:   mp.metrics.ActiveOutboundConnections,
		changes: changes,
	}
	waitForChange := func(want float64) {
		t.Helper()
		select {
		case delta := <-changes:
			require.Equal(t, want, delta)
		case <-time.After(time.Second):
			t.Fatal("broadcast routine did not update the active connection count")
		}
	}
	reactor := NewReactor(config.Mempool, mp, true)
	sw := p2p.NewSwitch(config.P2P, nil)
	sw.AddReactor("MEMPOOL", reactor)
	require.NoError(t, reactor.Start())
	t.Cleanup(func() { assert.NoError(t, reactor.Stop()) })

	// Keep sync pending across reconnects, as in continuous block sync.
	for i := 0; i < 20; i++ {
		peer := mock.NewPeer(nil)
		peer.Set(types.PeerStateKey, peerState{1})
		reactor.InitPeer(peer)
		reactor.AddPeer(peer)
		waitForChange(1)
		require.NoError(t, peer.Stop())
		reactor.RemovePeer(peer, nil)
		waitForChange(-1)
		require.True(t, reactor.WaitSync())
	}
}

func TestBroadcastTxRoutineStopsWhileWaiting(t *testing.T) {
	for _, waitSync := range []bool{true, false} {
		stage := "peer_state"
		if waitSync {
			stage = "sync"
		}
		for _, stopPeer := range []bool{true, false} {
			stop := "reactor"
			if stopPeer {
				stop = "peer"
			}
			t.Run(stage+"/"+stop, func(t *testing.T) {
				reactor := NewReactor(cfg.TestConfig().Mempool, nil, waitSync)
				require.NoError(t, reactor.Start())
				t.Cleanup(func() {
					if reactor.IsRunning() {
						assert.NoError(t, reactor.Stop())
					}
				})
				peer := mock.NewPeer(nil)
				if waitSync {
					peer.Set(types.PeerStateKey, peerState{1})
				}
				t.Cleanup(func() {
					if peer.IsRunning() {
						assert.NoError(t, peer.Stop())
					}
				})
				done := make(chan struct{})
				go func() {
					reactor.broadcastTxRoutine(peer)
					close(done)
				}()
				require.Never(t, func() bool {
					select {
					case <-done:
						return true
					default:
						return false
					}
				}, 50*time.Millisecond, time.Millisecond)
				if stopPeer {
					require.NoError(t, peer.Stop())
				} else {
					require.NoError(t, reactor.Stop())
				}
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("broadcast routine did not exit")
				}
			})
		}
	}
}

func TestReactorBroadcastAfterSync(t *testing.T) {
	reactors, switches := makeAndConnectReactorsWithWaitSync(cfg.TestConfig(), 2, true)
	t.Cleanup(func() {
		for _, sw := range switches {
			assert.NoError(t, sw.Stop())
		}
	})
	for _, reactor := range reactors {
		for _, peer := range reactor.Switch.Peers().Copy() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}
	txs := addRandomTxs(t, reactors[0].mempool, 3, UnknownPeerID)
	require.Never(t, func() bool { return reactors[1].mempool.Size() != 0 },
		50*time.Millisecond, time.Millisecond)
	for _, reactor := range reactors {
		reactor.EnableInOutTxs()
		reactor.EnableInOutTxs()
	}
	waitForTxsOnReactors(t, txs, reactors)
}
