import Foundation
import KafkaManagerCore

/// Tries a connection once and reports how many brokers answered. Never writes anything.
public protocol ConnectionTester: Sendable {
    func test(_ connection: ClusterConnection, credentials: Credentials?) async throws -> Int
}

/// Tests with a throwaway client from the same factory the sessions use. The client is not wrapped in the read-only
/// guard because it only connects and reads cluster metadata, and it is always disconnected afterwards, whether
/// the test succeeds, fails, times out or is cancelled.
public struct ClientConnectionTester: ConnectionTester {
    public static let defaultTimeout = Duration.seconds(10)

    private let factory: ClientFactory
    private let timeout: Duration
    private let sleep: @Sendable (Duration) async throws -> Void

    public init(
        factory: @escaping ClientFactory,
        timeout: Duration = ClientConnectionTester.defaultTimeout,
        sleep: @escaping @Sendable (Duration) async throws -> Void = { try await Task.sleep(for: $0) }
    ) {
        self.factory = factory
        self.timeout = timeout
        self.sleep = sleep
    }

    public func test(_ connection: ClusterConnection, credentials: Credentials?) async throws -> Int {
        let client = factory(connection)
        let timeout = timeout
        let sleep = sleep
        let result: Result<Int, any Error>
        do {
            let count = try await withThrowingTaskGroup(of: Int.self) { group in
                group.addTask {
                    try await client.connect(connection, credentials: credentials)
                    return try await client.describeCluster().brokerCount
                }
                group.addTask {
                    try await sleep(timeout)
                    throw KafkaError.timeout
                }
                defer { group.cancelAll() }
                guard let first = try await group.next() else { throw KafkaError.timeout }
                return first
            }
            result = .success(count)
        } catch {
            result = .failure(error)
        }
        await client.disconnect()
        return try result.get()
    }
}
