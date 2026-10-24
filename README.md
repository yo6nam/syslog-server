# Syslog Server

A modern **Syslog Server** written in Go that receives, stores, filters, and displays syslog messages from network devices. Includes real-time web UI, dual storage, an alerting system, and advanced security features.

---

## Table of Contents

* [What is it?](#what-is-it)
* [Core Features](#core-features)

  * [Message Reception](#-message-reception)
  * [Dual Storage](#-dual-storage)
  * [Web Interface](#-web-interface)
  * [Security & Control](#-security--control)
  * [Alerting System](#-alerting-system)
* [How It Works](#how-it-works)
* [Architecture](#architecture)
* [Key Technologies](#key-technologies)
* [Duplicate Suppression](#duplicate-suppression)
* [Configuration Example](#configuration-example)
* [Development](#development)
* [Use Cases](#use-cases)
* [Production Ready](#production-ready)
* [TL;DR](#tldr)

---

## What is it?

A modern **Syslog Server** written in Go that receives, stores, and displays syslog messages from network devices (routers, firewalls, servers, etc.). It includes a real-time web interface and a flexible alerting system.

---

## Core Features

### 📨 Message Reception

* **TCP & UDP listeners** on port 514 (standard syslog)
* UDP is read into a large socket buffer and a bounded queue with a single consumer, so a burst does not get dropped silently by the kernel and messages are processed **in arrival order** (drops, if the queue still overflows, are counted in `/api/stats`)
* Parses both **RFC3164** and **RFC5424** formats
* Compatible with all syslog-capable devices (Cisco, Linux, BSD, etc.)

### 💾 Dual Storage

* **File storage**: Saves messages as JSON, one file per day (`YYYY-MM-DD.json`)
* **MySQL storage**: Optional for large volumes and complex queries
* Both can run simultaneously (multi-storage mode)

### 🌐 Web Interface

* **Real-time view** via WebSocket feed
* **Search & Filter**: by content, hostname, severity, date range
* **Pagination**: 50/100/200 messages per page
* **Severity color coding** (Emergency = red, Critical = orange, etc.)
* Fully responsive for desktop and mobile

### 🛡️ Security & Control

#### Rate Limiting

* Per-IP rate limiting (e.g., 100 msg/sec)
* Burst handling (e.g., 200 instant messages allowed)
* Prevents log floods or misconfigured devices

#### Ban List

* Block specific IP addresses
* Block specific hostnames
* Dropped messages are silently ignored

#### Content Filtering

* Exclude patterns: **case-insensitive substring match** on the message content (not regular expressions)
* Useful for noise you never want to see (e.g., `"missed interrupt"`)
* For noise you *do* want to know about but not 100 times, use [Duplicate Suppression](#duplicate-suppression)

#### Duplicate Suppression

* Identical repeating messages are collapsed into one line plus a summary, like rsyslog's "last message repeated N times" ([details](#duplicate-suppression))

#### Web Access Control

* Optional HTTP Basic authentication for the UI, the API and the WebSocket (`web.basic_auth_user` / `web.basic_auth_password`)
* WebSocket connections are accepted only from the page served by this server (same-origin), so a random website cannot read your logs through your browser
* No CORS headers unless `web.cors_allow_origin` is set

### 🚨 Alerting System

* Webhook notifications for critical messages
* Configurable severities (`emergency`, `critical`)
* Rate limiting per hostname **and severity** (e.g., max 1 alert/hour for each), so a minor `critical` does not hide a later `emergency` from the same host
* Severity names in the config are case-insensitive
* Sends JSON payloads (Slack, Discord, custom)
* Asynchronous: does not block message flow

---

## How It Works

```
Device (router/firewall)
    ↓ syslog UDP/TCP :514          (UDP: socket buffer → queue → single worker)
Syslog Server
    ├─→ Ban List (IP)
    ├─→ Rate Limiter (per IP)
    ├─→ Message Parser (RFC3164 / RFC5424)
    ├─→ Ban List (hostname)
    ├─→ Content Filters (exclude patterns)
    ├─→ Duplicate Suppression  ── repeats counted, summary emitted later
    ├─→ Alerting (webhooks)
    ├─→ Storage (MySQL + File)
    └─→ WebSocket Broadcast (real-time UI)
```

---

## Architecture

### Backend (Go)

* `main.go` – Startup, orchestration, graceful shutdown
* `syslog/` – TCP/UDP listeners, message parsing
* `storage/` – MySQL and file storage backends
* `websocket/` – Real-time pub/sub hub
* `web/` – HTTP API + static asset serving
* `alerting/` – Webhook sender with rate limiting
* `types/` – Configuration & shared structs

### Frontend (Vanilla JS)

* `static/index.html` – Single-page UI
* `static/syslog-client.js` – WebSocket client, search, filters, pagination
* Auto-reconnect using exponential backoff

---

## Key Technologies

* **Go 1.19+**
* **MySQL 8.0** (optional)
* **WebSocket** for real-time updates
* **RFC3164 / RFC5424** parsing
* **Gorilla WebSocket**
* **Context-aware** timeouts and graceful shutdown

---

## Duplicate Suppression

Some sources emit the same line dozens of times per second, for example a kernel driver:

```
[404590.323487] xradio TXRX-ERR: [RX] IF=0, Receive failure: 4.
[404590.326062] xradio TXRX-ERR: [RX] IF=0, Receive failure: 4.
[404590.621651] xradio TXRX-ERR: [RX] IF=0, Receive failure: 4.
...
```

This is usually well under the rate limit (which only caps *volume*), yet it fills the live view and the history with one line. With duplicate suppression enabled (the default):

1. Each message gets a fingerprint: source IP, hostname, facility, severity, tag and content, **after removing kernel uptime stamps** such as `[404590.323487]`. Other numbers are kept, so `Receive failure: 4` and `Receive failure: 5` stay separate lines, as do `user 123` and `user 124`.
2. The **first** occurrence passes immediately (stored, broadcast, alerted).
3. Identical repeats inside the window are only counted.
4. One summary line is stored and shown when the flood has been quiet for `window_seconds`, or at least every `max_interval_seconds` while it keeps going:

```
xradio TXRX-ERR: [RX] IF=0, Receive failure: 4. (repeated 23 times in 3s)
```

Pending summaries are flushed on shutdown. Summaries are not alerted again. No schema change is involved: the count is part of the message text, so search and export work as usual.

```json
"dedup": {
  "enabled": true,
  "window_seconds": 10,
  "max_interval_seconds": 60,
  "max_entries": 10000,
  "normalize_patterns": ["session \\d+"]
}
```

* `normalize_patterns`: extra regular expressions removed before comparing (here: session ids). Invalid patterns are ignored with a warning.
* `max_entries` bounds memory. When the table is full, new messages are simply not tracked (they pass through; nothing is lost).
* Statistics (`total_suppressed`, `total_summaries`, `dedup_tracked`) are exposed on `/api/stats`.
* Set `"enabled": false` to store every message.

---

## Configuration Example

```json
{
  "mysql": { "enabled": true, "host": "localhost" },
  "ports": { "syslog_tcp": "0.0.0.0:514", "web": ":8080" },
  "rate_limiter": { "messages_per_second": 100, "burst_size": 200 },
  "ban_list": { "ips": ["192.168.1.100"], "hostnames": ["spam-host"] },
  "filters": { "exclude_patterns": ["missed interrupt"] },
  "alerting": {
    "enabled": true,
    "webhook_url": "https://hooks.slack.com/...",
    "severities": ["emergency", "critical"],
    "rate_limit_minutes": 60
  }
}
```

A complete file with every option and its default is in [`config.example.json`](config.example.json). Copy it to `config.json` (which is git-ignored, since it can hold secrets). If `config.json` is missing at startup, a default one is written (permissions `0600`).

Other options worth knowing:

| Option | Default | Purpose |
|---|---|---|
| `syslog.udp_read_buffer_bytes` | 4 MiB | UDP socket buffer (the OS may cap it, see `net.core.rmem_max`) |
| `syslog.udp_queue_size` | 8192 | packets waiting for processing |
| `syslog.max_tcp_connections` | 1000 | concurrent TCP clients |
| `syslog.log_every_message` | false | one log line per received message (noisy at volume) |
| `web.basic_auth_user` / `web.basic_auth_password` | empty | protect UI, API and WebSocket |
| `web.cors_allow_origin` | empty | allow one external origin to call the API |
| `web.allowed_origins` | empty | origins allowed to open the WebSocket besides same-origin, e.g. `["https://logs.example.com"]` (needed behind a reverse proxy that does not preserve `Host`) |

Invalid values (for example a rate limit of 0) are repaired at startup and reported as warnings.

**Linux:** the kernel silently caps `syslog.udp_read_buffer_bytes` at `net.core.rmem_max` (208 KiB by default), and a burst larger than the buffer is dropped before the server can read it. The server logs a warning at startup when that happens. To get the full 4 MiB:

```bash
sudo sysctl -w net.core.rmem_max=4194304
echo 'net.core.rmem_max=4194304' | sudo tee /etc/sysctl.d/90-syslog-server.conf
```

Dropped datagrams show up as `RcvbufErrors` in `/proc/net/snmp` (`netstat -su`).

---

## Behind a reverse proxy (Apache example)

The UI uses relative URLs, including the WebSocket (`<page directory>/ws`), so it works under a prefix such as `/syslog/`. Keep the WebSocket **under the same prefix** as the UI, so that whatever protects the UI (here Basic auth) protects the live stream too:

```apache
<Location /syslog>
    AuthType Basic
    AuthName "syslog"
    AuthBasicProvider file
    AuthUserFile "/etc/apache2/.htpasswd"
    Require valid-user
</Location>
RewriteRule ^/syslog$ /syslog/ [R]

# most specific first: the WebSocket, then the rest
ProxyPass        "/syslog/ws" "ws://BACKEND:8088/ws"
ProxyPassReverse "/syslog/ws" "ws://BACKEND:8088/ws"
ProxyPass        "/syslog/"   "http://BACKEND:8088/" connectiontimeout=5 timeout=30
```

Apache sends the backend's own `Host`, while the browser sends `Origin: https://logs.example.com`, so list that origin in `config.json`:

```json
"web": { "allowed_origins": ["https://logs.example.com"] }
```

(or add `ProxyPreserveHost On` inside the `<Location>` and no list is needed). Do **not** expose a separate `/ws` outside the protected location, and restrict the backend port (for example `ufw allow from <proxy ip> to any port 8088`) so the proxy cannot be bypassed.

---

## Development

```bash
gofmt -l .                 # must print nothing (CI enforces it)
go vet ./...
go test -race -count=1 ./...
```

* The web UI is embedded in the binary from `web/static/` with `//go:embed`. If that directive is ever lost (for instance by a tool that strips comments), the binary still compiles but serves 404; `web/server_test.go` fails in that case.
* Every push runs the tests before building the Linux and Windows binaries.

---

## Use Cases

* **Network Monitoring** – Centralize logs from all network devices
* **Security** – Detect attacks or anomalies using critical alerts
* **Debugging** – Search through thousands of messages instantly
* **Compliance** – Persistent storage for audit requirements
* **Operations** – Real-time troubleshooting in the browser

---

## Production Ready

* ✔ Per-IP rate limiting
* ✔ Ban list support
* ✔ Graceful shutdown (listeners stop first, pending summaries are flushed, then storage closes)
* ✔ Ordered, buffered UDP ingestion with visible drop counters
* ✔ Duplicate suppression with summaries
* ✔ Optional authentication and same-origin WebSocket
* ✔ Context timeouts for all DB queries
* ✔ SQL injection protection
* ✔ Optimized DB connection pooling
* ✔ Count caching for performance
* ✔ Thread-safe WebSocket hub
* ✔ Goroutine cleanup (no memory leaks)

---

## TL;DR

Receives syslog → filters noise → stores in MySQL/JSON → displays live in web UI → triggers alerts for critical issues. 🚀

---
