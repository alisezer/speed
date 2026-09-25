//! Download mirror pool and nearest-server selection.

use crate::latency;
use futures_util::future::join_all;
use reqwest::{Client, StatusCode, header::RANGE};
use std::{
    cmp::Ordering,
    net::Ipv4Addr,
    time::{Duration, Instant},
};

pub struct Server {
    pub city: &'static str,
    pub provider: &'static str,
    pub url: &'static str,
}

const fn s(city: &'static str, provider: &'static str, url: &'static str) -> Server {
    Server { city, provider, url }
}

/// Public test-file mirrors that allow large, repeated, parallel downloads. Hetzner
/// comes first: if probing fails entirely, the head of this list is the fallback.
/// Vultr's looking-glass hosts are left out: they allow one connection per IP and
/// then answer 503 for a long while.
pub static POOL: &[Server] = &[
    s("Falkenstein", "Hetzner", "https://fsn1-speed.hetzner.com/10GB.bin"),
    s("Nuremberg", "Hetzner", "https://nbg1-speed.hetzner.com/10GB.bin"),
    s("Helsinki", "Hetzner", "https://hel1-speed.hetzner.com/10GB.bin"),
    s("Ashburn", "Hetzner", "https://ash-speed.hetzner.com/10GB.bin"),
    s("Hillsboro", "Hetzner", "https://hil-speed.hetzner.com/10GB.bin"),
    s("Singapore", "Hetzner", "https://sin-speed.hetzner.com/10GB.bin"),
    s("London", "Linode", "https://speedtest.london.linode.com/100MB-london.bin"),
    s("Paris", "Linode", "https://speedtest.paris.linode.com/100MB-paris.bin"),
    s("Amsterdam", "Linode", "https://speedtest.amsterdam.linode.com/100MB-amsterdam.bin"),
    s("Frankfurt", "Linode", "https://speedtest.frankfurt.linode.com/100MB-frankfurt.bin"),
    s("Milan", "Linode", "https://speedtest.milan.linode.com/100MB-milan.bin"),
    s("Madrid", "Linode", "https://speedtest.madrid.linode.com/100MB-madrid.bin"),
    s("Stockholm", "Linode", "https://speedtest.stockholm.linode.com/100MB-stockholm.bin"),
    s("Newark", "Linode", "https://speedtest.newark.linode.com/100MB-newark.bin"),
    s("Washington", "Linode", "https://speedtest.washington.linode.com/100MB-washington.bin"),
    s("Atlanta", "Linode", "https://speedtest.atlanta.linode.com/100MB-atlanta.bin"),
    s("Miami", "Linode", "https://speedtest.miami.linode.com/100MB-miami.bin"),
    s("Chicago", "Linode", "https://speedtest.chicago.linode.com/100MB-chicago.bin"),
    s("Dallas", "Linode", "https://speedtest.dallas.linode.com/100MB-dallas.bin"),
    s("Toronto", "Linode", "https://speedtest.toronto1.linode.com/100MB-toronto1.bin"),
    s("Seattle", "Linode", "https://speedtest.seattle.linode.com/100MB-seattle.bin"),
    s("Fremont", "Linode", "https://speedtest.fremont.linode.com/100MB-fremont.bin"),
    s("Los Angeles", "Linode", "https://speedtest.los-angeles.linode.com/100MB-los-angeles.bin"),
    s("São Paulo", "Linode", "https://speedtest.sao-paulo.linode.com/100MB-sao-paulo.bin"),
    s("Mumbai", "Linode", "https://speedtest.mumbai1.linode.com/100MB-mumbai1.bin"),
    s("Chennai", "Linode", "https://speedtest.chennai.linode.com/100MB-chennai.bin"),
    s("Singapore", "Linode", "https://speedtest.singapore.linode.com/100MB-singapore.bin"),
    s("Jakarta", "Linode", "https://speedtest.jakarta.linode.com/100MB-jakarta.bin"),
    s("Tokyo", "Linode", "https://speedtest.tokyo2.linode.com/100MB-tokyo2.bin"),
    s("Osaka", "Linode", "https://speedtest.osaka.linode.com/100MB-osaka.bin"),
    s("Sydney", "Linode", "https://speedtest.sydney.linode.com/100MB-sydney.bin"),
    s("Roubaix", "OVH", "https://proof.ovh.net/files/1Gb.dat"),
    s("Beauharnois", "OVH", "https://proof.ovh.ca/files/1Gb.dat"),
    s("London", "thinkbroadband", "http://ipv4.download.thinkbroadband.com/1GB.zip"),
];

/// Hetzner allows 2 connections per IP; the same cap keeps us polite elsewhere.
pub const CONNS_PER_SERVER: usize = 2;
/// How many of the nearest servers `speed test` downloads from.
pub const TEST_SERVERS: usize = 5;

pub struct Ranked {
    pub server: &'static Server,
    pub rtt_ms: Option<f64>,
}

fn host(url: &str) -> &str {
    url.split('/').nth(2).unwrap_or(url)
}

/// Ranks every server by round trip, nearest first, unreachable last, and says how
/// ("ping" or "http"). Ping is cheap for the mirrors, so it's preferred; HTTP timing
/// is the fallback where ICMP is blocked entirely.
pub async fn rank(client: &Client) -> (Vec<Ranked>, &'static str) {
    let (mut ranked, mut method) = (ping_all().await, "ping");
    if ranked.iter().all(|r| r.rtt_ms.is_none()) {
        let probes = POOL.iter().map(|server| async move { Ranked { server, rtt_ms: http_rtt(client, server.url).await } });
        (ranked, method) = (join_all(probes).await, "http");
    }
    ranked.sort_by(|a, b| match (a.rtt_ms, b.rtt_ms) {
        (Some(x), Some(y)) => x.total_cmp(&y),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    });
    (ranked, method)
}

/// Best of two echoes to every server in the pool.
async fn ping_all() -> Vec<Ranked> {
    let addrs = join_all(POOL.iter().map(|s| latency::resolve_v4(host(s.url)))).await;
    // Unresolvable hosts are left out of the batch and come back unreachable
    let targets: Vec<(usize, Ipv4Addr)> = addrs.iter().enumerate().filter_map(|(i, a)| Some((i, (*a)?))).collect();
    let ips: Vec<Ipv4Addr> = targets.iter().map(|&(_, a)| a).collect();
    let rtts = latency::ping_many(ips).await;
    let mut ranked: Vec<Ranked> = POOL.iter().map(|server| Ranked { server, rtt_ms: None }).collect();
    for (&(i, _), rtt) in targets.iter().zip(rtts) {
        ranked[i].rtt_ms = rtt;
    }
    ranked
}

/// Checks the nearest candidates with one request each, in parallel. Returns
/// `(index into ranked, outcome)` for every server checked, nearest first.
pub async fn verify(client: &Client, ranked: &[Ranked], n: usize) -> Vec<(usize, Result<(), String>)> {
    let candidates: Vec<usize> = (0..ranked.len()).filter(|&i| ranked[i].rtt_ms.is_some()).take(n * 2).collect();
    let checks = candidates.iter().map(|&i| async move {
        let res = tokio::time::timeout(Duration::from_secs(3), tiny_get(client, ranked[i].server.url))
            .await
            .unwrap_or_else(|_| Err("timed out".into()));
        (i, res)
    });
    join_all(checks).await
}

/// The `n` nearest servers that serve files, or the head of the pool if none do.
pub async fn nearest(client: &Client, n: usize) -> Vec<Ranked> {
    let (ranked, _) = rank(client).await;
    let ok: Vec<usize> = verify(client, &ranked, n).await.into_iter().filter(|(_, r)| r.is_ok()).map(|(i, _)| i).take(n).collect();
    if ok.is_empty() {
        return POOL.iter().take(n).map(|server| Ranked { server, rtt_ms: None }).collect();
    }
    let mut ranked: Vec<Option<Ranked>> = ranked.into_iter().map(Some).collect();
    ok.into_iter().filter_map(|i| ranked[i].take()).collect()
}

/// Round trip of a 1-byte request on an already-open connection. The first request
/// pays for TCP and TLS setup and isn't timed. TCP connect time alone is unreliable:
/// some network gateways complete handshakes locally.
pub async fn http_rtt(client: &Client, url: &str) -> Option<f64> {
    let probe = async {
        tiny_get(client, url).await.ok()?;
        timed_get(client, url).await
    };
    tokio::time::timeout(Duration::from_secs(3), probe).await.ok().flatten()
}

/// One timed 1-byte request, in milliseconds.
pub async fn timed_get(client: &Client, url: &str) -> Option<f64> {
    let t = Instant::now();
    tiny_get(client, url).await.ok()?;
    Some(t.elapsed().as_secs_f64() * 1000.0)
}

async fn tiny_get(client: &Client, url: &str) -> Result<(), String> {
    let resp = client.get(url).header(RANGE, "bytes=0-0").send().await.map_err(|e| {
        if e.is_connect() { "could not connect".to_string() } else { e.to_string() }
    })?;
    match resp.status() {
        StatusCode::PARTIAL_CONTENT => {}
        // A server that ignores Range would send the whole file; don't read it
        StatusCode::OK => return Err("no range support".into()),
        s => return Err(format!("HTTP {}", s.as_u16())),
    }
    resp.bytes().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Distinct cities in order, e.g. "London · Amsterdam · Frankfurt".
pub fn cities(servers: &[Ranked]) -> String {
    let mut seen: Vec<&str> = Vec::new();
    for r in servers {
        if !seen.contains(&r.server.city) {
            seen.push(r.server.city);
        }
    }
    seen.join(" · ")
}

/// `speed servers`: every mirror ranked by round trip, with the nearest checked
/// and the ones `speed test` would use marked.
pub async fn list() -> anyhow::Result<()> {
    use crate::ui::{self, Accent};
    let client = crate::net::client();
    let (ranked, method) = rank(&client).await;
    let checks = verify(&client, &ranked, TEST_SERVERS).await;
    let mut used = 0;
    let reachable = ranked.iter().filter(|r| r.rtt_ms.is_some()).count();
    let summary = format!("{reachable} of {} reachable · ranked by {method}", POOL.len());
    println!("  {}  {}\n", ui::bold("speed servers"), ui::dim(&summary));
    for (i, r) in ranked.iter().enumerate() {
        let (mark, note) = match checks.iter().find(|(j, _)| *j == i) {
            Some((_, Ok(()))) if used < TEST_SERVERS => {
                used += 1;
                (Accent::Cyan.paint("●"), String::new())
            }
            Some((_, Err(e))) => (ui::red("✗"), ui::red(&format!("  {e}"))),
            _ => (ui::dim("·"), String::new()),
        };
        let rtt = match r.rtt_ms {
            Some(ms) => format!("{:>9}", ui::ms(ms)),
            None => ui::dim(&format!("{:>9}", "—")),
        };
        println!("  {mark} {rtt}   {:<13} {:<15} {}{note}", r.server.city, r.server.provider, ui::dim(host(r.server.url)));
    }
    Ok(())
}
