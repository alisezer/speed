//! Round-trip latency: ICMP echo to Cloudflare's nearest edge when the OS allows
//! it unprivileged (macOS, most Linux), otherwise timed HTTP requests on a warm
//! connection to the nearest download server.

use crate::{net, servers};
use reqwest::Client;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    io::Read,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, Ordering},
    },
    time::{Duration, Instant},
};

const PAYLOAD: &[u8; 8] = b"speed-rs";
static SEQ: AtomicU16 = AtomicU16::new(0);

pub enum Pinger {
    Icmp(Ipv4Addr),
    Http { client: Client, url: String },
}

impl Pinger {
    /// ICMP if an echo gets through, else HTTP to `fallback_url`, else nothing.
    pub async fn new(fallback_url: Option<&str>) -> Option<Pinger> {
        if let Some(addr) = resolve_v4(net::CLOUDFLARE).await {
            let p = Pinger::Icmp(addr);
            if p.rtt().await.is_some() {
                return Some(p);
            }
        }
        let url = fallback_url?.to_string();
        // A dedicated client, so download streams can't take over its warm connection
        let client = net::client();
        // Also opens the connection that later samples reuse
        servers::http_rtt(&client, &url).await?;
        Some(Pinger::Http { client, url })
    }

    pub fn method(&self) -> &'static str {
        match self {
            Pinger::Icmp(_) => "icmp",
            Pinger::Http { .. } => "http",
        }
    }

    pub async fn rtt(&self) -> Option<f64> {
        match self {
            Pinger::Icmp(addr) => icmp_rtt(*addr).await,
            Pinger::Http { client, url } => {
                tokio::time::timeout(Duration::from_secs(3), servers::timed_get(client, url)).await.ok().flatten()
            }
        }
    }
}

/// Reserves a block of sequence numbers so concurrent pings never collide.
pub fn next_seqs(n: usize) -> u16 {
    SEQ.fetch_add(n as u16, Ordering::Relaxed)
}

/// One ICMP echo round trip in milliseconds; `None` if not permitted or no reply.
pub async fn icmp_rtt(addr: Ipv4Addr) -> Option<f64> {
    let seq = next_seqs(1);
    tokio::task::spawn_blocking(move || icmp_echo(addr, seq, Duration::from_millis(1500)))
        .await
        .ok()
        .flatten()
}

pub async fn resolve_v4(host: &str) -> Option<Ipv4Addr> {
    tokio::net::lookup_host((host, 0)).await.ok()?.find_map(|a| match a {
        SocketAddr::V4(v4) => Some(*v4.ip()),
        SocketAddr::V6(_) => None,
    })
}

/// One ICMP echo over an unprivileged datagram socket; `None` if not permitted or no reply.
fn icmp_echo(addr: Ipv4Addr, seq: u16, timeout: Duration) -> Option<f64> {
    icmp_batch(&[addr], seq, 1, timeout).into_iter().next().flatten()
}

/// Pings every address `rounds` times from a single socket and returns the best
/// round trip for each. One socket matters: with many concurrent ICMP sockets,
/// macOS delivers some replies to the wrong one and they're lost.
pub fn icmp_batch(addrs: &[Ipv4Addr], first_seq: u16, rounds: usize, timeout: Duration) -> Vec<Option<f64>> {
    let mut best = vec![None; addrs.len()];
    let Ok(mut sock) = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::ICMPV4)) else {
        return best;
    };
    // seq -> (address index, send time)
    let mut sent = std::collections::HashMap::new();
    for round in 0..rounds {
        for (i, addr) in addrs.iter().enumerate() {
            let seq = first_seq.wrapping_add((round * addrs.len() + i) as u16);
            if sock.send_to(&echo_request(seq), &SocketAddr::from((*addr, 0)).into()).is_ok() {
                sent.insert(seq, (i, Instant::now()));
            }
        }
    }

    let deadline = Instant::now() + timeout;
    let mut buf = [0u8; 1500];
    while !sent.is_empty() {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() || sock.set_read_timeout(Some(left)).is_err() {
            break;
        }
        let Ok(n) = sock.read(&mut buf) else { break };
        let mut reply = &buf[..n];
        // macOS includes the IP header; Linux doesn't
        if reply.first().is_some_and(|b| b >> 4 == 4) {
            reply = &reply[((reply[0] & 0x0f) as usize * 4).min(reply.len())..];
        }
        // Match on sequence and payload: Linux rewrites the identifier
        if reply.len() < 16 || reply[0] != 0 || &reply[8..16] != PAYLOAD {
            continue;
        }
        if let Some((i, t)) = sent.remove(&u16::from_be_bytes([reply[6], reply[7]])) {
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            best[i] = Some(best[i].map_or(ms, |b: f64| b.min(ms)));
        }
    }
    best
}

fn echo_request(seq: u16) -> [u8; 16] {
    let mut pkt = [0u8; 16];
    pkt[0] = 8; // echo request
    pkt[4..6].copy_from_slice(&(std::process::id() as u16).to_be_bytes());
    pkt[6..8].copy_from_slice(&seq.to_be_bytes());
    pkt[8..].copy_from_slice(PAYLOAD);
    let sum = checksum(&pkt);
    pkt[2..4].copy_from_slice(&sum.to_be_bytes());
    pkt
}

fn checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = data.chunks(2).map(|c| u16::from_be_bytes([c[0], *c.get(1).unwrap_or(&0)]) as u32).sum();
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

pub struct Latency {
    pub ms: f64,
    pub jitter_ms: f64,
}

pub async fn idle(p: &Pinger, samples: usize) -> Option<Latency> {
    let mut rtts = Vec::with_capacity(samples);
    for _ in 0..samples {
        if let Some(ms) = p.rtt().await {
            rtts.push(ms);
        }
    }
    if rtts.is_empty() {
        return None;
    }
    // Jitter: mean difference between consecutive samples
    let jitter_ms = if rtts.len() > 1 {
        rtts.windows(2).map(|w| (w[1] - w[0]).abs()).sum::<f64>() / (rtts.len() - 1) as f64
    } else {
        0.0
    };
    Some(Latency { ms: net::median(&mut rtts), jitter_ms })
}

/// Samples latency every 250 ms while a transfer runs, to expose bufferbloat.
pub async fn loaded_loop(p: Arc<Pinger>, out: Arc<Mutex<Vec<f64>>>) {
    loop {
        if let Some(ms) = p.rtt().await {
            out.lock().unwrap().push(ms);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
