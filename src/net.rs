//! Transfer loops, latency probes and server metadata.
//!
//! Throughput is counted in-process as bytes move through our own connections,
//! so other traffic on the machine doesn't skew the numbers.

use bytes::Bytes;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::net::TcpStream;

pub const CLOUDFLARE: &str = "speed.cloudflare.com";

/// Hetzner speed mirrors; each allows 2 connections per IP.
pub const HETZNER: [&str; 5] = ["ash", "hil", "nbg1", "fsn1", "hel1"];
pub const HETZNER_CONNS: usize = 2;

/// Large files on mirrors that don't rate-limit repeated downloads (for `watch`).
pub const WATCH_SOURCES: [&str; 2] = [
    "https://proof.ovh.net/files/1Gb.dat",
    "http://ipv4.download.thinkbroadband.com/1GB.zip",
];

pub const UPLOAD_STREAMS: usize = 4;
/// Cloudflare rejects larger bodies when several uploads run in parallel.
const UPLOAD_BODY: usize = 50 * 1024 * 1024;
const UPLOAD_CHUNK: usize = 256 * 1024;

pub fn hetzner_url(mirror: &str) -> String {
    format!("https://{mirror}-speed.hetzner.com/10GB.bin")
}

pub fn client() -> Client {
    Client::builder()
        // One TCP connection per stream; HTTP/2 would multiplex them onto one
        .http1_only()
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(5))
        .user_agent(concat!("speed/", env!("CARGO_PKG_VERSION")))
        .build()
        .expect("building HTTP client")
}

/// Shared counters for a group of transfer loops.
#[derive(Default)]
pub struct Stats {
    pub bytes: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl Stats {
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().unwrap().clone()
    }

    fn fail(&self, msg: String) {
        *self.last_error.lock().unwrap() = Some(msg);
    }
}

fn describe_status(s: StatusCode) -> String {
    if s == StatusCode::TOO_MANY_REQUESTS {
        "rate limited (HTTP 429), wait a minute".into()
    } else {
        format!("HTTP {}", s.as_u16())
    }
}

fn describe_error(e: &reqwest::Error) -> String {
    if e.is_connect() {
        "could not connect".into()
    } else if e.is_timeout() {
        "timed out".into()
    } else {
        e.to_string()
    }
}

/// Downloads `url` over and over, counting bytes as they arrive. Runs until aborted.
pub async fn download_loop(client: Client, url: String, stats: Arc<Stats>) {
    loop {
        match client.get(&url).send().await {
            Ok(mut resp) if resp.status().is_success() => loop {
                match resp.chunk().await {
                    Ok(Some(chunk)) => {
                        stats.bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    }
                    Ok(None) => break,
                    Err(e) => {
                        stats.fail(describe_error(&e));
                        break;
                    }
                }
            },
            Ok(resp) => {
                stats.fail(describe_status(resp.status()));
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => {
                stats.fail(describe_error(&e));
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Posts 50 MB bodies to Cloudflare in a loop, counting bytes as they're handed
/// to the connection so the live rate doesn't wait for a whole body to finish.
pub async fn upload_loop(client: Client, stats: Arc<Stats>) {
    let chunk = Bytes::from(vec![0u8; UPLOAD_CHUNK]);
    let url = format!("https://{CLOUDFLARE}/__up");
    loop {
        let sent = Arc::new(AtomicU64::new(0));
        let body = {
            let (chunk, stats, sent) = (chunk.clone(), stats.clone(), sent.clone());
            futures_util::stream::iter((0..UPLOAD_BODY / UPLOAD_CHUNK).map(move |_| {
                stats.bytes.fetch_add(UPLOAD_CHUNK as u64, Ordering::Relaxed);
                sent.fetch_add(UPLOAD_CHUNK as u64, Ordering::Relaxed);
                Ok::<_, std::io::Error>(chunk.clone())
            }))
        };
        let res = client
            .post(&url)
            .body(reqwest::Body::wrap_stream(body))
            .send()
            .await;
        let ok = match res {
            Ok(r) if r.status().is_success() => true,
            Ok(r) => {
                stats.fail(describe_status(r.status()));
                false
            }
            Err(e) => {
                stats.fail(describe_error(&e));
                false
            }
        };
        if !ok {
            // The server didn't accept this body, so don't credit it
            stats.bytes.fetch_sub(sent.load(Ordering::Relaxed), Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

pub async fn resolve(host: &str) -> Option<SocketAddr> {
    tokio::net::lookup_host((host, 443)).await.ok()?.next()
}

/// Round-trip time of a TCP handshake (SYN → SYN-ACK), no root needed unlike ICMP.
pub async fn tcp_rtt(addr: SocketAddr) -> Option<f64> {
    let t = Instant::now();
    match tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await {
        Ok(Ok(_)) => Some(t.elapsed().as_secs_f64() * 1000.0),
        _ => None,
    }
}

pub struct Latency {
    pub ms: f64,
    pub jitter_ms: f64,
}

pub async fn idle_latency(addr: SocketAddr, samples: usize) -> Option<Latency> {
    let mut rtts = Vec::with_capacity(samples);
    for _ in 0..samples {
        if let Some(ms) = tcp_rtt(addr).await {
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
    Some(Latency { ms: median(&mut rtts), jitter_ms })
}

/// Probes latency every 250 ms while a transfer runs, to expose bufferbloat.
pub async fn loaded_latency_loop(addr: SocketAddr, out: Arc<Mutex<Vec<f64>>>) {
    loop {
        if let Some(ms) = tcp_rtt(addr).await {
            out.lock().unwrap().push(ms);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    let n = v.len();
    if n % 2 == 1 { v[n / 2] } else { (v[n / 2 - 1] + v[n / 2]) / 2.0 }
}

#[derive(Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Meta {
    pub client_ip: Option<String>,
    pub as_organization: Option<String>,
    pub city: Option<String>,
    pub colo: Option<Colo>,
}

#[derive(Deserialize, Default, Clone)]
pub struct Colo {
    pub iata: Option<String>,
}

/// Who and where we are, per Cloudflare. Best effort: empty on any failure.
pub async fn meta(client: &Client) -> Meta {
    let req = client
        .get(format!("https://{CLOUDFLARE}/meta"))
        // Without a Referer the endpoint returns `{}`
        .header("Referer", format!("https://{CLOUDFLARE}/"))
        .timeout(Duration::from_secs(3))
        .send();
    match req.await {
        Ok(r) => r.json().await.unwrap_or_default(),
        Err(_) => Meta::default(),
    }
}
