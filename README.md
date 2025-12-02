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
* [Configuration Example](#configuration-example)
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

* Exclude patterns using regex-like filters
* Useful for repetitive noise (e.g., `"missed interrupt"`, `"xradio_wlan"`)

### 🚨 Alerting System

* Webhook notifications for critical messages
* Configurable severities (`emergency`, `critical`)
* Per-hostname rate limiting (e.g., max 1 alert/hour)
* Sends JSON payloads (Slack, Discord, custom)
* Asynchronous: does not block message flow

---

## How It Works

```
Device (router/firewall)
    ↓ syslog UDP/TCP :514
Syslog Server
    ├─→ Rate Limiter
    ├─→ Ban List Check
    ├─→ Content Filters
    ├─→ Message Parser (RFC3164 / RFC5424)
    ├─→ Storage (MySQL + File)
    ├─→ WebSocket Broadcast (real-time UI)
    └─→ Alerting (webhooks)
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

* **Go 1.21+**
* **MySQL 8.0** (optional)
* **WebSocket** for real-time updates
* **RFC3164 / RFC5424** parsing
* **Gorilla WebSocket**
* **Context-aware** timeouts and graceful shutdown

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
* ✔ Graceful shutdown (no message loss)
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
