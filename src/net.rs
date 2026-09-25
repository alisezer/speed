//! Transfer loops and server metadata.
//!
//! Throughput is counted in-process as bytes move through our own connections,
//! so other traffic on the machine doesn't skew the numbers.

use bytes::Bytes;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

pub const CLOUDFLARE: &str = "speed.cloudflare.com";

pub const UPLOAD_STREAMS: usize = 4;
/// Cloudflare rejects larger bodies when several uploads run in parallel.
const UPLOAD_BODY: usize = 50 * 1024 * 1024;
const UPLOAD_CHUNK: usize = 256 * 1024;

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
