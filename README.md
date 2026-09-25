# speed

Internet speed test and live throughput monitor for the terminal.

```
  speed  Vorboss Limited · City of London (LHR)

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
```

## How it measures

- **Throughput** is counted in-process on its own connections, so other traffic on the machine doesn't skew it. The final rate skips the first 1–2 seconds of ramp-up.
- **Download** pulls from Hetzner's speed mirrors (2 connections each); **upload** posts to Cloudflare's speed endpoint.
- **Latency** is TCP handshake time to Cloudflare's nearest edge (no root needed, unlike ICMP). **Loaded latency** is measured the same way during each transfer, which exposes bufferbloat.
- **`watch`** keeps parallel downloads running and samples the rate once a second. It uses a lot of data: about 3.75 GB per minute at 500 Mbps.

Cloudflare rate-limits repeated uploads (HTTP 429); wait a minute between runs.
