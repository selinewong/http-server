import NIO
import NIOHTTP1
import NIOConcurrencyHelpers

// MARK: - Configuration
let maxConcurrentConnections = 5000
let idleTimeoutSeconds: Int64 = 60

// MARK: - Thread-safe connection counter
final class ConnectionCounter {
    private let lock = NIOLock()
    private var count = 0

    func tryAcquire(max: Int) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if count >= max { return false }
        count += 1
        return true
    }

    func release() {
        lock.lock()
        defer { lock.unlock() }
        count -= 1
    }

    var current: Int {
        lock.lock()
        defer { lock.unlock() }
        return count
    }
}

let connectionCounter = ConnectionCounter()

// MARK: - Connection limiter (first in pipeline)
final class ConnectionLimiterHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    let maxConnections: Int
    var accepted = false

    init(maxConnections: Int) {
        self.maxConnections = maxConnections
    }

    func channelActive(context: ChannelHandlerContext) {
        if connectionCounter.tryAcquire(max: maxConnections) {
            accepted = true
            context.fireChannelActive()
        } else {
            // Backpressure: refuse the connection immediately.
            context.close(promise: nil)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if accepted {
            connectionCounter.release()
        }
        context.fireChannelInactive()
    }
}

// MARK: - HTTP handler
final class HTTPHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let reqPart = self.unwrapInboundIn(data)

        if case .end = reqPart {
            var headers = HTTPHeaders()
            headers.add(name: "Content-Type", value: "text/plain")
            headers.add(name: "Content-Length", value: "13")

            let head = HTTPResponseHead(version: .http1_1, status: .ok, headers: headers)
            context.write(self.wrapOutboundOut(.head(head)), promise: nil)

            var buffer = context.channel.allocator.buffer(capacity: 13)
            buffer.writeString("Hello, World\n")
            context.write(self.wrapOutboundOut(.body(.byteBuffer(buffer))), promise: nil)

            context.writeAndFlush(self.wrapOutboundOut(.end(nil)), promise: nil)
        }
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        // Idle timeout fires this event; close the channel.
        if event is IdleStateHandler.IdleStateEvent {
            context.close(promise: nil)
        }
        context.fireUserInboundEventTriggered(event)
    }
}

// MARK: - Server setup
let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
defer { try! group.syncShutdownGracefully() }

let bootstrap = ServerBootstrap(group: group)
    .serverChannelOption(ChannelOptions.backlog, value: 256)
    .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
    .childChannelInitializer { channel in
        // Pipeline order:
        // 1. ConnectionLimiterHandler — accept or reject at byte level
        // 2. HTTP encode/decode (from configureHTTPServerPipeline)
        // 3. IdleStateHandler — fires event after 60s of no reads
        // 4. HTTPHandler — generates response, catches idle event
        channel.pipeline.addHandler(ConnectionLimiterHandler(maxConnections: maxConcurrentConnections)).flatMap {
            channel.pipeline.configureHTTPServerPipeline()
        }.flatMap {
            channel.pipeline.addHandler(IdleStateHandler(readTimeout: .seconds(idleTimeoutSeconds)))
        }.flatMap {
            channel.pipeline.addHandler(HTTPHandler())
        }
    }
    .childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)

let channel = try bootstrap.bind(host: "0.0.0.0", port: 8080).wait()
print("Server running on http://localhost:8080/")
print("Max concurrent connections: \(maxConcurrentConnections)")
print("Idle timeout: \(idleTimeoutSeconds)s")
try channel.closeFuture.wait()
