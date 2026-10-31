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

    // Consumer groups
    func listGroups() async throws -> [GroupSummary]
    func describeGroup(_ id: String) async throws -> GroupDetail
}
