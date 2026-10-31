import Foundation

/// Decorator that enforces read-only clusters in Core, so no UI bug can write to production.
///
/// Every new write method on `KafkaClient` MUST be guarded here: call `try blockIfReadOnly()` before
/// forwarding it. `GuardedKafkaClientTests` classifies each protocol method as read or write and fails
/// on an unclassified one. The wrapped client is private on purpose; there is no way around the guard.
public actor GuardedKafkaClient: KafkaClient {
    private let inner: any KafkaClient
    private let clusterName: String
    private var readOnly: Bool

    public init(wrapping inner: any KafkaClient, clusterName: String, isReadOnly: Bool) {
        self.inner = inner
        self.clusterName = clusterName
        self.readOnly = isReadOnly
    }

    public static func defaultReadOnly(for environment: ClusterConnection.Environment) -> Bool {
        environment == .production
    }

    public var isReadOnly: Bool { readOnly }

    /// Takes effect on the next call.
    public func setReadOnly(_ value: Bool) {
        readOnly = value
    }

    // MARK: Writes (guarded)

    public func createTopic(_ spec: NewTopic) async throws {
        try blockIfReadOnly()
        try await inner.createTopic(spec)
    }

    public func deleteTopic(_ name: String) async throws {
        try blockIfReadOnly()
        try await inner.deleteTopic(name)
    }

    // MARK: Reads (passthrough)

    public func connect(_ cluster: ClusterConnection, credentials: Credentials?) async throws {
        try await inner.connect(cluster, credentials: credentials)
    }

    public func disconnect() async {
        await inner.disconnect()
    }

    public func describeCluster() async throws -> ClusterInfo {
        try await inner.describeCluster()
    }

    public func listTopics() async throws -> [TopicSummary] {
        try await inner.listTopics()
    }

    public func describeTopic(_ name: String) async throws -> TopicDetail {
        try await inner.describeTopic(name)
    }

    public nonisolated func fetchMessages(_ query: MessageQuery) -> AsyncThrowingStream<KafkaMessage, Error> {
        inner.fetchMessages(query)
    }

    public func listGroups() async throws -> [GroupSummary] {
        try await inner.listGroups()
    }

    public func describeGroup(_ id: String) async throws -> GroupDetail {
        try await inner.describeGroup(id)
    }

    private func blockIfReadOnly() throws {
        if readOnly { throw KafkaError.readOnly(clusterName: clusterName) }
    }
}
