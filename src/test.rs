//! `speed test`: idle latency, then download and upload for a fixed time each.

use crate::{
    net::{self, Stats},
    ui::{self, Accent},
};
use anyhow::Result;
use clap::Parser;
use serde::Serialize;
use std::{
    collections::VecDeque,
    io::{IsTerminal, Write},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Parser)]
pub struct Opts {
    /// Seconds to spend on each direction
    #[arg(short, long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(2..=120))]
    pub duration: u64,
    /// Skip the upload phase
    #[arg(long)]
    pub no_upload: bool,
    /// Print the result as JSON instead of the live display
    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize)]
struct Report {
    isp: Option<String>,
    ip: Option<String>,
    location: Option<String>,
    latency_ms: Option<f64>,
    jitter_ms: Option<f64>,
    download: PhaseResult,
    upload: Option<PhaseResult>,
}

#[derive(Serialize)]
struct PhaseResult {
    mbps: f64,
    peak_mbps: f64,
    bytes: u64,
    seconds: f64,
    loaded_latency_ms: Option<f64>,
    error: Option<String>,
}

#[derive(Clone, Copy)]
enum Dir {
    Down,
    Up,
}

impl Dir {
    fn label(self) -> &'static str {
        match self {
            Dir::Down => "Download",
            Dir::Up => "Upload",
        }
    }
    fn icon(self) -> &'static str {
        match self {
            Dir::Down => "↓",
            Dir::Up => "↑",
        }
    }
    fn accent(self) -> Accent {
        match self {
            Dir::Down => Accent::Cyan,
            Dir::Up => Accent::Magenta,
        }
    }
}

/// "Vorboss Limited · City of London (LHR)"
pub fn server_line(m: &net::Meta) -> String {
    let place = match (&m.city, m.colo.as_ref().and_then(|c| c.iata.as_ref())) {
        (Some(city), Some(iata)) => Some(format!("{city} ({iata})")),
        (Some(city), None) => Some(city.clone()),
        (None, Some(iata)) => Some(iata.clone()),
        (None, None) => None,
    };
    [m.as_organization.clone(), place].into_iter().flatten().collect::<Vec<_>>().join(" · ")
}

fn row_label(icon: &str, label: &str, accent: Accent) -> String {
    format!("  {} {}", accent.paint(icon), ui::bold(&format!("{label:<9}")))
}

pub async fn run(o: Opts) -> Result<()> {
    let live = std::io::stdout().is_terminal() && !o.json;
    let client = net::client();
    let mut out = std::io::stdout();

    if live {
        print!("{}\n  {}  {}\n\n", ui::HIDE_CURSOR, ui::bold("speed"), ui::dim("connecting…"));
        out.flush()?;
    }
    let (meta, addr) = tokio::join!(net::meta(&client), net::resolve(net::CLOUDFLARE));
    if live {
        let server = server_line(&meta);
        let mut head = format!("  {}", ui::bold("speed"));
        if !server.is_empty() {
            head += &format!("  {}", ui::dim(&server));
        }
        // Replace the "connecting…" header (two lines up)
        print!("\x1b[2A{}{head}\n\n", ui::CLEAR_LINE);
        print!("{}{}", row_label("◷", "Latency", Accent::Cyan), ui::dim("measuring…"));
        out.flush()?;
    }

    let idle = match addr {
        Some(a) => net::idle_latency(a, 10).await,
        None => None,
    };
    if live {
        let text = match &idle {
            Some(l) => format!("{}   {}", ui::bold(&format!("{:>11}", ui::ms(l.ms))),ui::dim(&format!("jitter {}", ui::ms(l.jitter_ms)))),
            None => ui::yellow("unavailable"),
        };
        println!("{}{}{text}", ui::CLEAR_LINE, row_label("◷", "Latency", Accent::Cyan));
    }

    let secs = Duration::from_secs(o.duration);
    let idle_ms = idle.as_ref().map(|l| l.ms);
    let download = phase(Dir::Down, &client, secs, addr, idle_ms, live).await?;
    let upload = if o.no_upload {
        None
    } else {
        Some(phase(Dir::Up, &client, secs, addr, idle_ms, live).await?)
    };

    if live {
        print!("\n{}", ui::SHOW_CURSOR);
    } else {
        let report = Report {
            isp: meta.as_organization.clone(),
            ip: meta.client_ip.clone(),
            location: Some(server_line(&net::Meta { as_organization: None, ..meta.clone() }))
                .filter(|s| !s.is_empty()),
            latency_ms: idle.as_ref().map(|l| round1(l.ms)),
            jitter_ms: idle.as_ref().map(|l| round1(l.jitter_ms)),
            download,
            upload,
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
    }
    out.flush()?;
    Ok(())
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

/// Runs one direction for `dur`, drawing a live line, and returns the result.
async fn phase(
    dir: Dir,
    client: &reqwest::Client,
    dur: Duration,
    lat_addr: Option<SocketAddr>,
    idle_ms: Option<f64>,
    live: bool,
) -> Result<PhaseResult> {
    let stats = Arc::new(Stats::default());
    let loaded = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    match dir {
        Dir::Down => {
            for m in net::HETZNER {
                for _ in 0..net::HETZNER_CONNS {
                    tasks.push(tokio::spawn(net::download_loop(client.clone(), net::hetzner_url(m), stats.clone())));
                }
            }
        }
        Dir::Up => {
            for _ in 0..net::UPLOAD_STREAMS {
                tasks.push(tokio::spawn(net::upload_loop(client.clone(), stats.clone())));
            }
        }
    }
    if let Some(a) = lat_addr {
        tasks.push(tokio::spawn(net::loaded_latency_loop(a, loaded.clone())));
    }

    let accent = dir.accent();
    let start = Instant::now();
    let deadline = start + dur;
    // (time, bytes) samples covering the last second, for a smoothed live rate
    let mut window: VecDeque<(Instant, u64)> = VecDeque::from([(start, 0)]);
    let mut history: Vec<f64> = Vec::new();
    let mut peak = 0.0f64;
    // Where the steady state begins; the final rate is measured from here.
    // Upload needs longer: socket send buffers keep absorbing data for a while.
    let settle = Duration::from_secs(match dir {
        Dir::Down => 1,
        Dir::Up => 2,
    });
    let mut settled: Option<(Instant, u64)> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.tick().await;

    loop {
        tick.tick().await;
        let now = Instant::now();
        let b = stats.bytes();
        window.push_back((now, b));
        while window.len() > 2 && now - window[1].0 >= Duration::from_secs(1) {
            window.pop_front();
        }
        let (t0, b0) = window[0];
        let mbps = b.saturating_sub(b0) as f64 * 8.0 / (now - t0).as_secs_f64() / 1e6;
        history.push(mbps);
        // Connection setup and send-buffer fill distort the first moments
        if now - start >= settle {
            peak = peak.max(mbps);
            settled.get_or_insert((now, b));
        }
        if live {
            draw_live(dir, &history, mbps, (now - start).as_secs_f64(), dur.as_secs_f64())?;
        }
        if now >= deadline {
            break;
        }
    }
    for t in &tasks {
        t.abort();
    }

    let seconds = start.elapsed().as_secs_f64();
    let bytes = stats.bytes();
    let mut lat = loaded.lock().unwrap().clone();
    let loaded_ms = (!lat.is_empty()).then(|| net::median(&mut lat));
    // Under 1 MB means nothing meaningful got through
    let error = (bytes < 1_000_000).then(|| stats.last_error().unwrap_or_else(|| "no data transferred".into()));
    // Skip the settle period: TCP ramp-up drags the average down, and on upload the
    // instant fill of socket send buffers would count bytes not yet on the wire
    let (t1, b1) = settled.unwrap_or((start, 0));
    let mbps = bytes.saturating_sub(b1) as f64 * 8.0 / t1.elapsed().as_secs_f64() / 1e6;

    if live {
        let mut line = row_label(dir.icon(), dir.label(), accent);
        match &error {
            Some(e) => line += &ui::red(&format!("failed · {e}")),
            None => {
                line += &accent.paint_bold(&format!("{:>11}", ui::rate(mbps)));
                let mut details = vec![format!("peak {}", ui::rate(peak.max(mbps))), ui::bytes(bytes)];
                if let Some(l) = loaded_ms {
                    let delta = idle_ms.map(|i| format!(" (+{})", ui::ms((l - i).max(0.0)))).unwrap_or_default();
                    details.push(format!("loaded latency {}{delta}", ui::ms(l)));
                }
                // Row label + rate take 27 columns; on narrow terminals give each detail its own line
                let joined = details.join(" · ");
                if 27 + joined.chars().count() <= ui::term_width() {
                    line += &format!("   {}", ui::dim(&joined));
                } else {
                    for d in &details {
                        line += &format!("\n{}{}", " ".repeat(15), ui::dim(d));
                    }
                }
            }
        }
        println!("{}{line}", ui::CLEAR_LINE);
    }

    Ok(PhaseResult {
        mbps: round1(mbps),
        peak_mbps: round1(peak.max(mbps)),
        bytes,
        seconds: (seconds * 100.0).round() / 100.0,
        loaded_latency_ms: loaded_ms.map(round1),
        error,
    })
}

fn draw_live(dir: Dir, history: &[f64], mbps: f64, elapsed: f64, total: f64) -> Result<()> {
    let accent = dir.accent();
    let width = ui::term_width();
    let mut line = row_label(dir.icon(), dir.label(), accent);
    line += &accent.paint_bold(&format!("{:>11}", ui::rate(mbps)));
    // label(14) + rate(11) + gaps; the sparkline and bar only if there's room
    if width >= 72 {
        let spark = &history[history.len().saturating_sub(24)..];
        let max = spark.iter().cloned().fold(1.0, f64::max);
        line += &format!("  {}", accent.paint(&format!("{:<24}", ui::sparkline(spark, max))));
    }
    if width >= 48 {
        line += &format!("  {} {}", ui::progress(elapsed / total, 12, accent), ui::dim(&format!("{:>2.0}s", elapsed.min(total))));
    }
    print!("{}{line}", ui::CLEAR_LINE);
    std::io::stdout().flush()?;
    Ok(())
}
