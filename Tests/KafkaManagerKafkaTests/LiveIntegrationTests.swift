import Foundation
import KafkaManagerCore
import Testing
@testable import KafkaManagerKafka

/// Runs against a real broker; skipped unless `KM_ITEST=1` (use `scripts/dev.sh itest`). The broker defaults to
/// plaintext `localhost:9092`, override with `KM_ITEST_BOOTSTRAP`. Only topics named `km-itest-*` are created, and
/// only those are ever deleted (also leftovers from earlier runs, at the start of each test).
extension LiveSerial {
@Suite(.enabled(if: ProcessInfo.processInfo.environment["KM_ITEST"] == "1"))
struct IntegrationTests {
    private static let prefix = "km-itest-"
    private static let bootstrap = ProcessInfo.processInfo.environment["KM_ITEST_BOOTSTRAP"] ?? "localhost:9092"
    private let cluster = ClusterConnection(name: "itest", bootstrapServers: bootstrap)

    private static func uniqueName() -> String { prefix + UUID().uuidString.prefix(8).lowercased() }

    private func connected(timeout: Duration = .seconds(10)) async throws -> LiveKafkaClient {
        let client = LiveKafkaClient(timeout: timeout)
        try await client.connect(cluster, credentials: nil)
        for topic in try await client.listTopics() where topic.name.hasPrefix(Self.prefix) {
            try await client.deleteTopic(topic.name)
        }
        return client
    }

    /// Topic changes reach the metadata asynchronously; poll briefly instead of sleeping a fixed time.
    private func eventually(_ attempts: Int = 40, _ check: () async throws -> Bool) async throws -> Bool {
        for _ in 0..<attempts {
            if try await check() { return true }
            try await Task.sleep(for: .milliseconds(250))
        }
        return false
    }

    @Test func connectDescribeClusterAndList() async throws {
        let client = try await connected()
        let info = try await client.describeCluster()
        #expect(info.brokerCount >= 1)
        let topics = try await client.listTopics()
        #expect(topics.allSatisfy { $0.partitionCount >= 0 && $0.replicationFactor >= 0 })
        await client.disconnect()
        #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
    }

    @Test func createDescribeAndDeleteATopic() async throws {
        let client = try await connected()
        let name = Self.uniqueName()
        defer { Task { try? await client.deleteTopic(name) } }

        try await client.createTopic(NewTopic(name: name, partitions: 3, replicationFactor: 1, config: ["retention.ms": "3600000"]))
        #expect(try await eventually { try await client.listTopics().contains { $0.name == name } })
        let listed = try await client.listTopics().first { $0.name == name }
        #expect(listed?.partitionCount == 3)
        #expect(listed?.replicationFactor == 1)

        // Leaders are elected a moment after creation.
        var detail = try await client.describeTopic(name)
        _ = try await eventually { detail = try await client.describeTopic(name); return detail.partitions.allSatisfy { $0.leader != nil } }
        #expect(detail.partitions.map(\.id) == [0, 1, 2])
        for partition in detail.partitions {
            #expect(partition.leader != nil)
            #expect(partition.replicas.count == 1)
            #expect(partition.isr == partition.replicas)
            #expect(partition.startOffset == 0)
            #expect(partition.endOffset == 0)
        }
        let retention = detail.configEntries.first { $0.name == "retention.ms" }
        #expect(retention?.value == "3600000")
        #expect(retention?.isDefault == false)
        let cleanup = detail.configEntries.first { $0.name == "cleanup.policy" }
        #expect(cleanup?.isDefault == true)
        #expect(detail.config["retention.ms"] == "3600000")

        try await client.deleteTopic(name)
        #expect(try await eventually {
            do { _ = try await client.describeTopic(name); return false } catch KafkaError.unknownTopic { return true }
        })
        await client.disconnect()
        #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
    }

    @Test func creatingAnExistingTopicFails() async throws {
        let client = try await connected()
        let name = Self.uniqueName()
        defer { Task { try? await client.deleteTopic(name) } }
        try await client.createTopic(NewTopic(name: name, partitions: 1, replicationFactor: 1))
        await #expect(throws: KafkaError.topicAlreadyExists(name)) {
            try await client.createTopic(NewTopic(name: name, partitions: 1, replicationFactor: 1))
        }
        try await client.deleteTopic(name)
        await client.disconnect()
    }

    @Test func invalidRequestsAreMapped() async throws {
        let client = try await connected()
        let name = Self.uniqueName()
        await #expect(throws: KafkaError.unknownTopic(name)) { try await client.describeTopic(name) }
        await #expect(throws: KafkaError.unknownTopic(name)) { try await client.deleteTopic(name) }
        var invalid: KafkaError?
        do { try await client.createTopic(NewTopic(name: name, partitions: 1, replicationFactor: 9)) } catch let error as KafkaError { invalid = error }
        guard case .clusterError(let code, _) = invalid else {
            Issue.record("expected clusterError, got \(String(describing: invalid))")
            return
        }
        #expect(code == KafkaError.invalidReplicationFactorCode)
        await client.disconnect()
        // The failed attempts must not have created anything.
        let again = try await connected()
        #expect(try await again.listTopics().allSatisfy { $0.name != name })
        await again.disconnect()
    }

    @Test func unreachableHostFailsWithinTheTimeout() async throws {
        let client = LiveKafkaClient(timeout: .seconds(2))
        let dead = ClusterConnection(name: "dead", bootstrapServers: "localhost:1")
        let started = ContinuousClock.now
        var failure: KafkaError?
        do { try await client.connect(dead, credentials: nil) } catch let error as KafkaError { failure = error }
        #expect(ContinuousClock.now - started < .seconds(5))
        #expect(failure == .unreachable(host: "localhost:1"), "\(String(describing: failure))")
    }

    @Test func saslAgainstAPlaintextPortIsAMappedError() async throws {
        let client = LiveKafkaClient(timeout: .seconds(3))
        let wrong = ClusterConnection(
            name: "wrong", bootstrapServers: Self.bootstrap, security: .saslPlaintext, saslMechanism: .scramSha256, username: "nobody")
        var failure: KafkaError?
        do { try await client.connect(wrong, credentials: Credentials(username: "nobody", password: "itest-secret-value")) } catch let error as KafkaError { failure = error }
        let error = try #require(failure, "connect should fail")
        switch error {
        case .authenticationFailed, .unreachable, .timeout: break
        default: Issue.record("unexpected mapping: \(error)")
        }
        #expect(!"\(error)".contains("itest-secret-value"))
        #expect(!error.userMessage.contains("itest-secret-value"))
    }
}
}
