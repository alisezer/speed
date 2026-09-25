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

## For scripts and agents

```
speed test --json -d 5        # one JSON object (also the default when stdout isn't a terminal)
speed watch --json -d 30      # JSON Lines: {"type":"start"}, {"type":"sample"} per second, {"type":"summary"}
timeout 30 speed watch --json # SIGTERM or Ctrl+C also ends with a summary line
```

Exit codes: `0` ok, `1` error, `2` bad arguments, `3` a measurement got no data. On `3`, the `error` fields say why (e.g. `rate limited (HTTP 429), wait a minute`). `speed watch --json` without `-d` runs until stopped.

## How it measures

- **Throughput** is counted in-process on its own connections, so other traffic on the machine doesn't skew it. The final rate skips the first 1–2 seconds of ramp-up.
- **Download** uses the 5 nearest of 34 public mirrors (Hetzner, Linode, OVH, thinkbroadband) across North and South America, Europe, Asia and Oceania, 2 connections each. Servers are ranked by ping (cheap for the mirrors), then the nearest get one HTTP request each to confirm they serve files. `speed servers` shows the ranking. **Upload** posts to Cloudflare, which is anycast and so already nearby everywhere.
- **Latency** is ICMP echo to Cloudflare's nearest edge, over an unprivileged socket (no root needed on macOS; on Linux when `net.ipv4.ping_group_range` allows it). Otherwise it uses the system `ping` binary, and failing that, times HTTP requests on a warm connection, marked `(http)`. TCP handshake time isn't used: some network gateways complete handshakes locally, which hides the real round trip. **Loaded latency** is measured the same way during each transfer, which exposes bufferbloat.
- **`watch`** keeps parallel downloads running from the nearest servers and samples the rate once a second. It uses a lot of data: about 3.75 GB per minute at 500 Mbps.

Cloudflare rate-limits repeated uploads (HTTP 429); wait a minute between runs.

## License

MIT
