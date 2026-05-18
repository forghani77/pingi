# pingi

`pingi` is a high-performance ICMP echo (ping) scanner written in Go, designed for large-scale network discovery. It is optimized to handle millions of targets with minimal memory overhead by using a streaming architecture and concurrent worker pools.

## Features

- **High Performance:** Concurrent worker pool for fast scanning.
- **Low Memory Footprint:** Streams IPs and CIDRs from files directly to workers; never loads the entire target list into memory.
- **CIDR Support:** Automatically expands CIDR ranges (e.g., `192.168.1.0/24`) on-the-fly.
- **Auto-Privilege Detection:** Automatically switches between unprivileged (UDP) and privileged (raw sockets) modes based on environment capabilities.
- **Simple Help Menu:** Easy-to-use command-line interface.

## Installation

Ensure you have Go installed, then clone the repository and build:

```bash
go build -o pingi main.go
```

## Usage

```bash
./pingi [options] [ips_file]
```

### Options

- `-f string`: File containing list of IPs or CIDRs to scan (default "ips.list").
- `-w int`: Number of concurrent workers (default 1000).
- `-t duration`: Ping timeout per target (default 1s).
- `-p`: Force privileged mode (uses raw sockets, may require `sudo` or `CAP_NET_RAW`).

### Examples

**Scan a file with default settings:**
```bash
./pingi targets.txt
```

**High-speed scan with 2000 workers and 500ms timeout:**
```bash
./pingi -w 2000 -t 500ms targets.txt
```

**Scan using pipes:**
```bash
cat targets.txt | ./pingi
```
*(Note: You can also use `-` to explicitly specify stdin, e.g., `./pingi -f -`)*

## Why `pingi`?

Unlike many simple ping scripts that load all targets into memory or spawn thousands of system processes, `pingi` uses Go's lightweight goroutines and a producer-consumer streaming model. This makes it suitable for scanning massive target lists (e.g., 4M+ IPs) without crashing or exhausting system resources.

## License

MIT
