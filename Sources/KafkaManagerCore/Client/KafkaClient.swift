import Foundation

/// The only surface features use to talk to Kafka. Implemented by the live client, the fake, and guards.
public protocol KafkaClient: Sendable {
    func connect(_ cluster: ClusterConnection, credentials: Credentials?) async throws
    func disconnect() async
    /// Read-only cluster metadata (broker count). Requires a connection.
    func describeCluster() async throws -> ClusterInfo

    // Topics
    func listTopics() async throws -> [TopicSummary]
    func describeTopic(_ name: String) async throws -> TopicDetail
    func createTopic(_ spec: NewTopic) async throws
    func deleteTopic(_ name: String) async throws

    // Messages
    func fetchMessages(_ query: MessageQuery) -> AsyncThrowingStream<KafkaMessage, Error>

    // Produce
    /// Write operation: sends one message and returns where the broker stored it. Never creates the topic.
    func produce(_ request: ProduceRequest) async throws -> ProduceResult

    // Consumer groups
    func listGroups() async throws -> [GroupSummary]
    func describeGroup(_ id: String) async throws -> GroupDetail
    /// Read only: what resetting the group's committed offsets to `target` would do. Changes nothing.
    func previewOffsetReset(group: String, target: OffsetResetTarget, scope: OffsetResetScope) async throws -> OffsetResetPreview
    /// Write operation: commits the plan's offsets for the group. The broker refuses a group that has members
    /// (`KafkaError.isNonEmptyGroup`); per-partition failures come back in the result.
    func resetOffsets(group: String, plan: OffsetResetPlan) async throws -> OffsetResetResult
}
