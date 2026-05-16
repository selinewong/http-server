# Swift HTTP/1.1 Server

A high-performance HTTP/1.1 server built on SwiftNIO, demonstrating non-blocking I/O, async/await concurrency, connection limiting, and idle timeout handling.

## Features

- Event-driven HTTP/1.1 server using `MultiThreadedEventLoopGroup` (one event loop per core)
- **Connection cap (5,000)** enforced at the byte-level by a custom `ConnectionLimiterHandler` using a `NIOLock`-protected counter; over-limit connections are closed immediately (backpressure path)
- **60-second idle timeout** via `IdleStateHandler`; channels with no inbound activity for 60s are closed
- HTTP request/response pipeline via SwiftNIO's `configureHTTPServerPipeline()`

## Architecture

Channel pipeline (per connection):

1. `ConnectionLimiterHandler` — accepts or refuses at the byte level
2. HTTP encode/decode (`configureHTTPServerPipeline`)
3. `IdleStateHandler` — 60s read timeout
4. `HTTPHandler` — responds with 200 OK / "Hello, World"; closes channel on idle event

## Build and run

```bash
swift build
swift run
```

Server listens on `0.0.0.0:8080`.

## Benchmarks

Measured with `wrk` on WSL2 (Ubuntu 24.04, Windows host). File descriptor limit raised to 65535 via `ulimit -n 65535` on both server and client shells.

### Command

```bash
wrk -t<threads> -c<connections> -d30s --latency http://localhost:8080/
```

### Results

| Connections | Threads | Req/s   | p50    | p99    | Timeouts | Notes |
|-------------|---------|---------|--------|--------|----------|-------|
| 100         | 4       | 7,015   | 12ms   | 64ms   | 0        | Baseline |
| 1,000       | 4       | 9,581   | 89ms   | 250ms  | 418      | ~0.14% timeout rate |
| 2,500       | 8       | 10,001  | 113ms  | 211ms  | 1,077    | ~0.36% error rate |
| 3,000       | 8       | 9,057   | 113ms  | 227ms  | 1,187    | ~0.43% error rate |

Sustained ~3,000 concurrent connections for 30 seconds with sub-1% timeout rate. Comfortable steady-state is approximately 1,000–2,000 concurrent.

### Limitations

- Single-machine test (server and load generator on the same WSL2 host)
- Default Linux TCP tuning
- WSL2 may impose its own networking limits below native Linux

## Project structure
├── Package.swift           # SwiftPM manifest
├── Sources/http-server/
│   └── main.swift          # All handlers + server bootstrap
└── README.md

## Dependencies

- [swift-nio](https://github.com/apple/swift-nio) — `NIO`, `NIOHTTP1`, `NIOConcurrencyHelpers`