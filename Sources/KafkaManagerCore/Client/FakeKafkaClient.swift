import Foundation

/// In-memory `KafkaClient` for tests and previews, with failure and latency injection.
public actor FakeKafkaClient: KafkaClient {
    public enum Method: Hashable, Sendable {
        case connect, listTopics, describeTopic, createTopic, deleteTopic, fetchMessages, listGroups, describeGroup
    }

    /// A topic with its partition layout, config and stored messages.
    public struct SeedTopic: Sendable {
        public var detail: TopicDetail
        public var messages: [KafkaMessage]

        public init(detail: TopicDetail, messages: [KafkaMessage] = []) {
            self.detail = detail
            self.messages = messages
        }
    }

    private static let unknownGroupCode = 69
    public static let defaultHost = "localhost:9092"

    private var topics: [String: TopicDetail]
    private var messages: [String: [Int: [KafkaMessage]]]
    private var groups: [String: GroupDetail]
    private let expectedCredentials: Credentials?
    private var latency: Duration
    private var host: String
    private var isConnected = false
    private var nextFailure: KafkaError?
    private var persistentFailures: [Method: KafkaError] = [:]

    /// - Parameters:
    ///   - host: reported by `.unreachable` before any `connect`; afterwards the connected cluster's first bootstrap server is used.
    ///   - expectedCredentials: when set, `connect` throws `.authenticationFailed` unless the credentials match.
    ///   - latency: delay before each call completes, and between messages of a fetch.
    public init(
        topics: [SeedTopic] = [],
        groups: [GroupDetail] = [],
        host: String = FakeKafkaClient.defaultHost,
        expectedCredentials: Credentials? = nil,
        latency: Duration = .zero
    ) {
        self.topics = Dictionary(topics.map { ($0.detail.name, $0.detail) }) { _, last in last }
        self.messages = Dictionary(topics.map { ($0.detail.name, Self.groupByPartition($0.messages)) }) { _, last in last }
        self.groups = Dictionary(groups.map { ($0.id, $0) }) { _, last in last }
        self.host = host
        self.expectedCredentials = expectedCredentials
        self.latency = latency
    }

    // MARK: Injection

    /// The next call to any method throws `error`, once.
    public func failNext(_ error: KafkaError) {
        nextFailure = error
    }

    /// Every call to `method` throws `error` until cleared with `nil`.
    public func failing(_ method: Method, with error: KafkaError?) {
        persistentFailures[method] = error
    }

    public func setLatency(_ latency: Duration) {
        self.latency = latency
    }

    // MARK: Connection

    public func connect(_ cluster: ClusterConnection, credentials: Credentials?) async throws {
        try await begin(.connect, requiresConnection: false)
        host = Self.firstServer(of: cluster.bootstrapServers) ?? host
        if let expectedCredentials, credentials != expectedCredentials {
            throw KafkaError.authenticationFailed
        }
        isConnected = true
    }

    public func disconnect() async {
        isConnected = false
    }

    // MARK: Topics

    public func listTopics() async throws -> [TopicSummary] {
        try await begin(.listTopics)
        return topics.values
            .map { TopicSummary(name: $0.name, partitionCount: $0.partitions.count, replicationFactor: $0.partitions.first?.replicas.count ?? 0) }
            .sorted { $0.name < $1.name }
    }

    public func describeTopic(_ name: String) async throws -> TopicDetail {
        try await begin(.describeTopic)
        guard let detail = topics[name] else { throw KafkaError.unknownTopic(name) }
        return detail
    }

    public func createTopic(_ spec: NewTopic) async throws {
        try await begin(.createTopic)
        guard topics[spec.name] == nil else { throw KafkaError.topicAlreadyExists(spec.name) }
        let brokers = Array(0..<max(spec.replicationFactor, 1))
        let partitions = (0..<spec.partitions).map {
            PartitionInfo(id: $0, leader: brokers.first, replicas: brokers, isr: brokers, startOffset: 0, endOffset: 0)
        }
        topics[spec.name] = TopicDetail(name: spec.name, partitions: partitions, config: spec.config)
    }

    public func deleteTopic(_ name: String) async throws {
        try await begin(.deleteTopic)
        guard topics.removeValue(forKey: name) != nil else { throw KafkaError.unknownTopic(name) }
        messages[name] = nil
    }

    // MARK: Messages

    /// Emits messages ordered by partition then offset. `latest(N)` yields the N newest messages
    /// (by timestamp) among the selected partitions, capped by `limit`.
    public nonisolated func fetchMessages(_ query: MessageQuery) -> AsyncThrowingStream<KafkaMessage, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    let (result, delay) = try await self.select(query)
                    for message in result {
                        try Task.checkCancellation()
                        if delay > .zero { try await Task.sleep(for: delay) }
                        continuation.yield(message)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    private func select(_ query: MessageQuery) async throws -> ([KafkaMessage], Duration) {
        try await begin(.fetchMessages)
        guard topics[query.topic] != nil else { throw KafkaError.unknownTopic(query.topic) }
        guard query.limit > 0 else { return ([], latency) }

        let byPartition = messages[query.topic] ?? [:]
        let partitionIds = query.partition.map { [$0] } ?? byPartition.keys.sorted()
        let candidates = partitionIds.flatMap { byPartition[$0] ?? [] }

        let selected: [KafkaMessage]
        switch query.start {
        case .earliest:
            selected = Array(candidates.prefix(query.limit))
        case .offset(let offset):
            selected = Array(candidates.filter { $0.offset >= offset }.prefix(query.limit))
        case .timestamp(let date):
            selected = Array(candidates.filter { $0.timestamp >= date }.prefix(query.limit))
        case .latest(let count):
            let newest = candidates
                .sorted { ($0.timestamp, $0.offset, $0.partition) > ($1.timestamp, $1.offset, $1.partition) }
                .prefix(max(0, min(count, query.limit)))
            selected = newest.sorted { ($0.partition, $0.offset) < ($1.partition, $1.offset) }
        }
        return (selected, latency)
    }

    // MARK: Groups

    public func listGroups() async throws -> [GroupSummary] {
        try await begin(.listGroups)
        return groups.values
            .map { GroupSummary(id: $0.id, state: $0.state, memberCount: $0.members.count) }
            .sorted { $0.id < $1.id }
    }

    public func describeGroup(_ id: String) async throws -> GroupDetail {
        try await begin(.describeGroup)
        guard let detail = groups[id] else {
            throw KafkaError.clusterError(code: Self.unknownGroupCode, message: "Group \(id) not found")
        }
        return detail
    }

    // MARK: Helpers

    /// Common prologue: latency (cancellable), injected failures, then the connection check.
    private func begin(_ method: Method, requiresConnection: Bool = true) async throws {
        if latency > .zero { try await Task.sleep(for: latency) }
        if let error = nextFailure {
            nextFailure = nil
            throw error
        }
        if let error = persistentFailures[method] { throw error }
        if requiresConnection, !isConnected { throw KafkaError.unreachable(host: host) }
    }

    private static func firstServer(of bootstrapServers: String) -> String? {
        bootstrapServers
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .first { !$0.isEmpty }
    }

    private static func groupByPartition(_ messages: [KafkaMessage]) -> [Int: [KafkaMessage]] {
        Dictionary(grouping: messages, by: \.partition).mapValues { $0.sorted { $0.offset < $1.offset } }
    }
}
