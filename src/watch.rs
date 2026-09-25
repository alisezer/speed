//! `speed watch`: keeps parallel downloads running and charts throughput live.

use crate::{
    net::{self, Stats},
    servers,
    test::server_line,
    ui::{self, Accent},
};
use anyhow::Result;
use clap::Parser;
use serde::Serialize;
use serde_json::json;
use std::{
    io::{IsTerminal, Write},
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Parser)]
pub struct Opts {
    /// Parallel download streams
    #[arg(short, long, default_value_t = 4, value_parser = clap::value_parser!(u64).range(1..=32))]
    pub streams: u64,
    /// Stop after this many seconds instead of running until Ctrl+C or SIGTERM
    #[arg(short, long)]
    pub duration: Option<u64>,
    /// Print JSON Lines: a "start" object, a "sample" each second, then a "summary"
    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize)]
struct ServerInfo {
    city: &'static str,
    provider: &'static str,
    rtt_ms: Option<f64>,
}

/// Resolves on Ctrl+C or SIGTERM, so `timeout 30 speed watch` still prints a summary.
async fn stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            },
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

const CHART_HEIGHT: usize = 6;
/// Samples before this many seconds are ramp-up and don't count toward min/max.
const WARMUP: usize = 2;

struct Tally {
    history: Vec<f64>,
    min: f64,
    max: f64,
}

impl Tally {
    /// Mean of the per-second rates after warm-up, consistent with min/max.
    fn avg(&self) -> f64 {
        let settled = self.history.get(WARMUP..).filter(|s| !s.is_empty()).unwrap_or(&self.history);
        settled.iter().sum::<f64>() / settled.len().max(1) as f64
    }
}

/// Returns whether any data got through.
pub async fn run(o: Opts) -> Result<bool> {
    let live = std::io::stdout().is_terminal() && !o.json;
    let client = net::client();
    let mut out = std::io::stdout();

    // Enough servers that none gets more than its connection cap, up to 5 for spread
    let streams = o.streams as usize;
    let want = streams.min(servers::TEST_SERVERS).max(streams.div_ceil(servers::CONNS_PER_SERVER));
    let (meta, nearest) = tokio::join!(net::meta(&client), servers::nearest(&client, want));
    let server = server_line(&meta);
    let details = [server, format!("{} streams via {}", o.streams, servers::cities(&nearest))]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" · ");
    let servers: Vec<ServerInfo> = nearest
        .iter()
        .map(|r| ServerInfo { city: r.server.city, provider: r.server.provider, rtt_ms: r.rtt_ms.map(round1) })
        .collect();
    if o.json {
        let start = json!({
            "type": "start",
            "isp": meta.as_organization,
            "streams": o.streams,
            "duration": o.duration,
            "servers": servers,
        });
        println!("{start}");
    } else {
        println!("  {}  {}\n", ui::bold("speed watch"), ui::dim(&details));
    }
    if live {
        print!("{}", ui::HIDE_CURSOR);
    }

    let stats = Arc::new(Stats::default());
    let tasks: Vec<_> = (0..streams)
        .map(|i| {
            let url = nearest[i % nearest.len()].server.url.to_string();
            tokio::spawn(net::download_loop(client.clone(), url, stats.clone()))
        })
        .collect();

    let start = Instant::now();
    let stop_at = o.duration.map(|d| start + Duration::from_secs(d));
    let mut tick = tokio::time::interval_at((start + Duration::from_secs(1)).into(), Duration::from_secs(1));
    let stop = stop_signal();
    tokio::pin!(stop);
    let (mut prev_t, mut prev_b) = (start, 0u64);
    let mut tally = Tally { history: Vec::new(), min: f64::INFINITY, max: 0.0 };
    let mut drawn = 0;

    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = &mut stop => break,
        }
        let (now, b) = (Instant::now(), stats.bytes());
        // Divide by the real elapsed time, not a nominal 1s
        let mbps = (b - prev_b) as f64 * 8.0 / (now - prev_t).as_secs_f64() / 1e6;
        (prev_t, prev_b) = (now, b);
        tally.history.push(mbps);
        if tally.history.len() > WARMUP {
            tally.min = tally.min.min(mbps);
            tally.max = tally.max.max(mbps);
        }
        let elapsed = now - start;
        let avg = tally.avg();

        let settled = tally.history.len() > WARMUP;
        if o.json {
            // avg/min/max are null until the warm-up seconds have passed
            let sample = json!({
                "type": "sample",
                "t": tally.history.len(),
                "mbps": round1(mbps),
                "avg_mbps": settled.then(|| round1(avg)),
                "min_mbps": settled.then(|| round1(tally.min)),
                "max_mbps": settled.then(|| round1(tally.max)),
                "bytes": b,
                "error": if mbps < 0.1 { stats.last_error() } else { None },
            });
            println!("{sample}");
        } else if live {
            drawn = draw(&tally, mbps, avg, elapsed, b, stats.last_error(), drawn)?;
        } else {
            let mut line = format!("{:>5}s  {mbps:>7.1} Mbps", tally.history.len());
            if settled {
                line += &format!("  avg {avg:.1}  min {:.1}  max {:.1}", tally.min, tally.max);
            }
            println!("{line}");
        }
        if stop_at.is_some_and(|s| now >= s) {
            break;
        }
    }
    for t in &tasks {
        t.abort();
    }

    let elapsed = start.elapsed();
    let b = stats.bytes();
    if live {
        print!("{}", ui::SHOW_CURSOR);
        if drawn > 0 {
            // Replace the running-stats line with the final summary
            print!("\x1b[2A\x1b[J");
        }
    }
    // Under 1 MB means nothing meaningful got through
    let ok = b >= 1_000_000;
    let error = (!ok).then(|| stats.last_error().unwrap_or_else(|| "no data transferred".into()));
    if o.json {
        let settled = tally.history.len() > WARMUP;
        let summary = json!({
            "type": "summary",
            "seconds": (elapsed.as_secs_f64() * 100.0).round() / 100.0,
            "avg_mbps": (!tally.history.is_empty()).then(|| round1(tally.avg())),
            "min_mbps": settled.then(|| round1(tally.min)),
            "max_mbps": settled.then(|| round1(tally.max)),
            "bytes": b,
            "servers": servers,
            "error": error,
        });
        println!("{summary}");
        return Ok(ok);
    }
    if tally.history.is_empty() {
        println!();
        return Ok(ok);
    }
    let avg = tally.avg();
    let line = format!(
        "  {}  avg {}   {}   {}",
        ui::bold(&ui::duration(elapsed.as_secs())),
        Accent::Cyan.paint_bold(&ui::rate(avg)),
        ui::dim(&format!("min {} · max {}", ui::rate(finite(tally.min)), ui::rate(tally.max))),
        ui::dim(&format!("{} downloaded", ui::bytes(b))),
    );
    println!("\n{line}");
    if let Some(e) = &error {
        println!("  {}", ui::red(&format!("failed · {e}")));
    }
    out.flush()?;
    Ok(ok)
}

fn finite(v: f64) -> f64 {
    if v.is_finite() { v } else { 0.0 }
}

/// Redraws the dashboard in place; returns how many lines it occupies.
fn draw(t: &Tally, mbps: f64, avg: f64, elapsed: Duration, bytes: u64, err: Option<String>, drawn: usize) -> Result<usize> {
    let width = ui::term_width().max(30);
    let mut lines = Vec::new();

    // Current rate, with elapsed time and data used on the right
    let now = Accent::Cyan.paint_bold(&ui::rate(mbps));
    let right = format!("{} · {}", ui::duration(elapsed.as_secs()), ui::bytes(bytes));
    let now_len = ui::rate(mbps).len();
    let gap = width.saturating_sub(2 + now_len + 4 + right.len() + 2).max(2);
    lines.push(format!("  {now} {}{}{}", ui::dim("now"), " ".repeat(gap), ui::dim(&right)));
    lines.push(String::new());

    // Chart with a y-axis on the left, scaled to the visible window's peak
    let axis_w = 6;
    let chart_w = width.saturating_sub(2 + axis_w + 2).max(10);
    let visible = &t.history[t.history.len().saturating_sub(chart_w)..];
    let scale = ui::nice_ceil(visible.iter().cloned().fold(0.0, f64::max));
    for (i, row) in ui::chart(visible, chart_w, CHART_HEIGHT, scale).iter().enumerate() {
        let label = match i {
            0 => format!("{:>5}", axis_value(scale)),
            _ if i == CHART_HEIGHT - 1 => format!("{:>5}", 0),
            _ => " ".repeat(5),
        };
        let tick = if i == 0 || i == CHART_HEIGHT - 1 { "┤" } else { "│" };
        lines.push(format!("  {} {}", ui::dim(&format!("{label}{tick}")), Accent::Cyan.paint(row)));
    }
    lines.push(format!("  {}{}", " ".repeat(axis_w), ui::dim(&format!("{:<w$}", "Mbps", w = chart_w.saturating_sub(4)))));

    // Running stats, or the latest error while nothing is flowing
    let (stats, stats_len) = if t.history.len() > WARMUP {
        let (a, lo, hi) = (ui::rate(avg), ui::rate(finite(t.min)), ui::rate(t.max));
        let len = format!("avg {a}   min {lo}   max {hi}").chars().count();
        (format!("avg {}   min {lo}   max {hi}", ui::bold(&a)), len)
    } else {
        (ui::dim("warming up…"), 11)
    };
    // Right-aligned hint, or the latest error while nothing is flowing
    let (status, status_len) = match err {
        Some(e) if mbps < 0.1 => {
            let s = format!("⚠ {e}");
            (ui::yellow(&s), s.chars().count())
        }
        _ => (ui::dim("Ctrl+C to stop"), 14),
    };
    let gap = width.saturating_sub(2 + stats_len + status_len + 1).max(3);
    lines.push(String::new());
    lines.push(format!("  {stats}{}{status}", " ".repeat(gap)));

    let mut frame = String::new();
    if drawn > 0 {
        frame += &format!("\x1b[{drawn}A");
    }
    for l in &lines {
        frame += &format!("{}{l}\n", ui::CLEAR_LINE);
    }
    frame += "\x1b[J"; // clear leftovers if the terminal shrank
    let mut out = std::io::stdout();
    out.write_all(frame.as_bytes())?;
    out.flush()?;
    Ok(lines.len())
}

fn axis_value(v: f64) -> String {
    if v >= 1000.0 { format!("{}G", v / 1000.0) } else { format!("{v}") }
}
