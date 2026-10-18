//! A TURN server reached over TCP that accepts the connection and then says nothing.
//!
//! This is what a captive proxy looks like from the inside: the connect succeeds, so the
//! relayer has a stream, and the Allocate goes out and is never answered. Two things must
//! hold anyway. Gathering has to finish — a reliable transaction waits 39.5s for an answer
//! (RFC 5389 §7.2.1), where UDP gives up after 7.8s, and a non-trickle caller waits with it.
//! And the driver has to keep working: writes to the stream are queued rather than awaited,
//! so a peer that accepts and never drains cannot stop reads, timers or `close()`.
//!
//! What the unit tests cover and this cannot is a send buffer that is actually full — that
//! needs megabytes through the socket, not a mock. Here the stall is in the *answer*, and
//! what is measured is that nothing waits on it for longer than it should.
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder,
    RTCIceGatheringState, RTCIceServer, RTCIceTransportPolicy,
};
use webrtc::runtime::{Sender, channel};

mod common;
use common::{block_on, runtime, timeout};

/// Gathering must end well inside the 39.5s a silent reliable transaction would take.
const GATHER_LIMIT: Duration = Duration::from_secs(20);

/// `close()` has a stuck stream to let go of, and no reason to wait for it.
const CLOSE_LIMIT: Duration = Duration::from_secs(5);

struct Handler {
    gather_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }
}

/// Accept every connection and never read a byte of it, holding each stream open.
fn silent_turn_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a listener");
    let addr = listener.local_addr().expect("its address");
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => held.push(stream),
                Err(_) => break,
            }
        }
    });
    addr
}

#[test]
fn a_silent_tcp_turn_server_does_not_hold_gathering_or_close() {
    block_on(async {
        let server = silent_turn_server();
        let runtime = runtime();
        let (gather_tx, mut gather_rx) = channel::<()>(1);

        let peer = PeerConnectionBuilder::new()
            .with_configuration(
                RTCConfigurationBuilder::new()
                    .with_ice_servers(vec![RTCIceServer {
                        urls: vec![format!("turn:{server}?transport=tcp")],
                        username: "user".to_owned(),
                        credential: "pass".to_owned(),
                    }])
                    .with_ice_transport_policy(RTCIceTransportPolicy::Relay)
                    .build(),
            )
            .with_handler(Arc::new(Handler { gather_tx }))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await
            .expect("a peer connection");

        let _dc = peer
            .create_data_channel("silent", None)
            .await
            .expect("a data channel");
        let offer = peer.create_offer(None).await.expect("an offer");
        let started = Instant::now();
        peer.set_local_description(offer)
            .await
            .expect("set_local_description");

        timeout(GATHER_LIMIT, gather_rx.recv())
            .await
            .expect("gathering must finish without an answer from the server");
        let gathering = started.elapsed();
        eprintln!("gathering finished in {gathering:?}");

        let sdp = peer.local_description().await.expect("a description");
        assert!(
            !sdp.sdp.contains("typ relay"),
            "this server granted no allocation, so there is no relay to offer:\n{}",
            sdp.sdp
        );

        let closing = Instant::now();
        timeout(CLOSE_LIMIT, peer.close())
            .await
            .expect("close must not wait for a stream that is not carrying anything")
            .expect("close");
        eprintln!("close returned in {:?}", closing.elapsed());
    });
}
