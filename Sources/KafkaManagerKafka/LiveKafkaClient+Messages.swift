import Foundation
import KafkaManagerCore
import os

extension LiveKafkaClient {
    /// Messages buffered between the reader thread and the consumer of the stream. When it is full the reader stops
    /// polling, so a slow consumer never lets memory grow.
    static let streamBuffer = 256

    /// Reads on a thread of its own (never the cooperative pool or the client's admin queue) and stops promptly when the
    /// stream is terminated. The result never contains more than `query.limit` messages.
    nonisolated public func fetchMessages(_ query: MessageQuery) -> AsyncThrowingStream<KafkaMessage, Error> {
        guard let settings = consumerSettings.withLock({ $0 }) else {
            return AsyncThrowingStream { $0.finish(throwing: KafkaError.unreachable(host: "the cluster")) }
        }
        let timeoutMs = self.timeoutMs
        return AsyncThrowingStream(KafkaMessage.self, bufferingPolicy: .bufferingOldest(Self.streamBuffer)) { continuation in
            let stopped = OSAllocatedUnfairLock(initialState: false)
            continuation.onTermination = { _ in stopped.withLock { $0 = true } }
            DispatchQueue(label: "kafka-manager.fetch").async {
                do {
                    try MessageReader.run(
                        settings: settings, query: query, timeoutMs: timeoutMs,
                        isStopped: { stopped.withLock { $0 } },
                        emit: { message in
                            while !stopped.withLock({ $0 }) {
                                switch continuation.yield(message) {
                                case .enqueued:
                                    FetchStats.enqueued.withLock { $0 += 1 }
                                    return true
                                case .terminated: return false
                                default: Thread.sleep(forTimeInterval: 0.005)
                                }
                            }
                            return false
                        })
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }
}

/// Counters for tests. Cheap enough to stay on.
enum FetchStats {
    /// Messages handed to a stream's buffer since launch.
    static let enqueued = OSAllocatedUnfairLock(initialState: 0)
}
