# speed

Internet speed test and live throughput monitor for the terminal.

```
  speed  Vorboss Limited · City of London (LHR)
  via London · Roubaix · Frankfurt · Nuremberg

  ◷ Latency       4.7 ms   jitter 0.7 ms
  ↓ Download  113.2 Mbps   peak 124.3 Mbps · 139 MB · loaded latency 189 ms (+185 ms)
  ↑ Upload    118.2 Mbps   peak 132.1 Mbps · 150 MB · loaded latency 107 ms (+103 ms)
```

## Install

```
cargo install --path .
```

## Usage

```
speed                  # latency, download, upload (10s each)
speed -d 5 --no-upload # test options work without the `test` subcommand
speed test --json      # machine-readable result
speed watch            # continuous download with a live chart; Ctrl+C for a summary
speed watch -s 8 -d 60 # 8 streams, stop after 60s
speed servers          # rank the download mirrors from where you are
```

## How it measures

- **Throughput** is counted in-process on its own connections, so other traffic on the machine doesn't skew it. The final rate skips the first 1–2 seconds of ramp-up.
- **Download** uses the 5 nearest of ~37 public mirrors (Hetzner, Vultr, Linode, OVH, thinkbroadband) across North America, Europe, Asia, Oceania, South America and Africa, 2 connections each. Servers are ranked by ICMP ping (cheap for the mirrors, some of which rate-limit HTTP), then the nearest get one HTTP request each to confirm they serve files. **Upload** posts to Cloudflare, which is anycast and so already nearby everywhere.
- **Latency** is ICMP echo to Cloudflare's nearest edge, over an unprivileged socket (no root needed on macOS and most Linux). Where ICMP isn't allowed it falls back to timing HTTP requests on a warm connection, marked `(http)`. TCP handshake time isn't used: some network gateways complete handshakes locally, which hides the real round trip. **Loaded latency** is measured the same way during each transfer, which exposes bufferbloat.
- **`watch`** keeps parallel downloads running from the nearest servers and samples the rate once a second. It uses a lot of data: about 3.75 GB per minute at 500 Mbps.

Cloudflare rate-limits repeated uploads (HTTP 429); wait a minute between runs.
