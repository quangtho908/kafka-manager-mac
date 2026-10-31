import Foundation
import os
import Testing
import KafkaManagerCore
import KafkaManagerUI

/// Counts factory calls and keeps the fakes it handed out. `OSAllocatedUnfairLock` is Sendable, so no unchecked
/// conformance is needed for the `@Sendable` factory.
final class FactoryProbe: Sendable {
    private let state = OSAllocatedUnfairLock(initialState: (connections: [ClusterConnection](), fakes: [FakeKafkaClient]()))
    private let make: @Sendable () -> FakeKafkaClient

    init(make: @escaping @Sendable () -> FakeKafkaClient = { FakeKafkaClient() }) {
        self.make = make
    }

    var factory: ClientFactory {
        { [self] connection in
            let fake = make()
            state.withLock {
                $0.connections.append(connection)
                $0.fakes.append(fake)
            }
            return fake
        }
    }

    var callCount: Int { state.withLock { $0.connections.count } }
    var connections: [ClusterConnection] { state.withLock { $0.connections } }
    var fakes: [FakeKafkaClient] { state.withLock { $0.fakes } }
}

/// A `sleep` replacement that waits until the test fires it; cancellation ends it like the real one.
actor SleepController {
    private var waiters: [Int: CheckedContinuation<Void, any Error>] = [:]
    private var cancelledEarly: Set<Int> = []
    private var nextID = 0
    private(set) var requested: [Duration] = []

    nonisolated var sleep: @Sendable (Duration) async throws -> Void {
        { [self] duration in try await wait(duration) }
    }

    var pendingCount: Int { waiters.count }

    struct Snapshot: Equatable {
        let requested: Int
        let pending: Int
    }

    func snapshot() -> Snapshot {
        Snapshot(requested: requested.count, pending: waiters.count)
    }

    /// Ends the sleep that was requested `index`-th (0-based), if it is still waiting.
    func fire(_ index: Int) {
        waiters.removeValue(forKey: index)?.resume()
    }

    private func wait(_ duration: Duration) async throws {
        let id = nextID
        nextID += 1
        requested.append(duration)
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                if cancelledEarly.remove(id) != nil {
                    continuation.resume(throwing: CancellationError())
                } else {
                    waiters[id] = continuation
                }
            }
        } onCancel: {
            Task { await self.cancel(id) }
        }
    }

    private func cancel(_ id: Int) {
        if let continuation = waiters.removeValue(forKey: id) {
            continuation.resume(throwing: CancellationError())
        } else {
            cancelledEarly.insert(id)
        }
    }
}

/// Lets other tasks run until `condition` holds; bounded so a bug fails the test instead of hanging it.
@MainActor
func settle(_ condition: @MainActor () async -> Bool) async -> Bool {
    for _ in 0..<2_000 {
        if await condition() { return true }
        await Task.yield()
    }
    return await condition()
}

@MainActor
func expectSettles(sourceLocation: SourceLocation = #_sourceLocation, _ condition: @MainActor () async -> Bool) async {
    let settled = await settle(condition)
    #expect(settled, "condition never became true", sourceLocation: sourceLocation)
}
