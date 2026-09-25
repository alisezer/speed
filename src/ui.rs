//! Terminal styling and drawing helpers.

use std::{
    io::IsTerminal,
    sync::atomic::{AtomicBool, Ordering},
};

pub const HIDE_CURSOR: &str = "\x1b[?25l";
pub const SHOW_CURSOR: &str = "\x1b[?25h";
/// Return to column 0 and clear the line.
pub const CLEAR_LINE: &str = "\r\x1b[2K";

static COLOR: AtomicBool = AtomicBool::new(true);

pub fn init_color() {
    let on = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    COLOR.store(on, Ordering::Relaxed);
}

fn sgr(code: &str, s: &str) -> String {
    if COLOR.load(Ordering::Relaxed) {
        format!("\x1b[{code}m{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

pub fn bold(s: &str) -> String { sgr("1", s) }
pub fn dim(s: &str) -> String { sgr("2", s) }
pub fn red(s: &str) -> String { sgr("31", s) }
pub fn yellow(s: &str) -> String { sgr("33", s) }

#[derive(Clone, Copy)]
pub enum Accent {
    Cyan,
    Magenta,
}

impl Accent {
    pub fn paint(self, s: &str) -> String {
        match self {
            Accent::Cyan => sgr("36", s),
            Accent::Magenta => sgr("35", s),
        }
    }
    pub fn paint_bold(self, s: &str) -> String {
        match self {
            Accent::Cyan => sgr("1;36", s),
            Accent::Magenta => sgr("1;35", s),
        }
    }
}

pub fn term_width() -> usize {
    // Pseudo-terminals can report 0 columns
    crossterm::terminal::size().ok().map(|(w, _)| w as usize).filter(|&w| w > 0).unwrap_or(80)
}

/// "482.3 Mbps", "1.24 Gbps", "850 kbps"
pub fn rate(mbps: f64) -> String {
    if mbps >= 1000.0 {
        format!("{:.2} Gbps", mbps / 1000.0)
    } else if mbps >= 1.0 {
        format!("{mbps:.1} Mbps")
    } else {
        format!("{:.0} kbps", mbps * 1000.0)
    }
}

pub fn bytes(n: u64) -> String {
    let n = n as f64;
    if n >= 1e9 { format!("{:.2} GB", n / 1e9) } else { format!("{:.0} MB", n / 1e6) }
}

pub fn duration(secs: u64) -> String {
    match secs {
        s if s >= 3600 => format!("{}h{:02}m", s / 3600, s % 3600 / 60),
        s if s >= 60 => format!("{}m{:02}s", s / 60, s % 60),
        s => format!("{s}s"),
    }
}

pub fn ms(v: f64) -> String {
    if v < 10.0 { format!("{v:.1} ms") } else { format!("{v:.0} ms") }
}

const BLOCKS: [char; 9] = [' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

/// One-row sparkline scaled to `max`.
pub fn sparkline(vals: &[f64], max: f64) -> String {
    vals.iter()
        .map(|&v| {
            let i = if max > 0.0 { (v / max * 8.0).round() as usize } else { 0 };
            BLOCKS[i.clamp(1, 8)]
        })
        .collect()
}

/// Thin progress bar: accent-colored filled part, dim remainder.
pub fn progress(frac: f64, width: usize, accent: Accent) -> String {
    let filled = ((frac.clamp(0.0, 1.0) * width as f64).round() as usize).min(width);
    format!("{}{}", accent.paint(&"━".repeat(filled)), dim(&"─".repeat(width - filled)))
}

/// Rounds up to a readable axis maximum (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8 × 10^k),
/// fine-grained enough that the chart uses most of its height.
pub fn nice_ceil(v: f64) -> f64 {
    if v <= 0.0 {
        return 1.0;
    }
    let mag = 10f64.powf(v.log10().floor());
    for step in [1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0] {
        if v <= step * mag {
            return step * mag;
        }
    }
    10.0 * mag
}

/// Multi-row bar chart, newest value on the right. Returns `height` rows of
/// exactly `width` chars (uncolored); values are right-aligned, padded on the left.
pub fn chart(vals: &[f64], width: usize, height: usize, max: f64) -> Vec<String> {
    let vals = &vals[vals.len().saturating_sub(width)..];
    let pad = width - vals.len();
    let units: Vec<usize> = vals
        .iter()
        .map(|&v| if max > 0.0 { (v / max * (height * 8) as f64).round() as usize } else { 0 })
        .collect();
    (0..height)
        .map(|row| {
            // Row 0 is the top; the bottom row covers units 0..8
            let base = (height - 1 - row) * 8;
            let mut line = " ".repeat(pad);
            line.extend(units.iter().map(|&u| BLOCKS[u.saturating_sub(base).min(8)]));
            line
        })
        .collect()
}
