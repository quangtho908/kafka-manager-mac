import Foundation
import KafkaManagerCore
import Testing
@testable import KafkaManagerKafka

@Suite("Test-owned group guard")
struct TestOwnedGroupGuardTests {
    @Test func onlyKmItestGroupsPassTheGuard() throws {
        try requireTestOwnedGroup("km-itest-g-abc123")
        for id in ["SyncInvoiceOfflineLocal", "orders-service", "km-itest-g-", "km-itest-abc", "KM-ITEST-G-1", "", " km-itest-g-x"] {
            #expect(throws: (any Error).self) { try requireTestOwnedGroup(id) }
        }
    }
}

/// Resets offsets on a real broker; skipped unless `KM_ITEST=1`. Only a `km-itest-g-*` group the test creates is ever
/// joined or reset, on a `km-itest-*` topic the test creates; both are removed afterwards.
extension LiveSerial {
    @Suite(.enabled(if: ProcessInfo.processInfo.environment["KM_ITEST"] == "1"))
    struct ResetOffsetsIntegrationTests {
        private static let bootstrap = ProcessInfo.processInfo.environment["KM_ITEST_BOOTSTRAP"] ?? "localhost:9092"
        private let cluster = ClusterConnection(name: "itest", bootstrapServers: bootstrap)

        private func connected() async throws -> LiveKafkaClient {
            let client = LiveKafkaClient(timeout: .seconds(10))
            try await client.connect(cluster, credentials: nil)
            for topic in try await client.listTopics() where topic.name.hasPrefix("km-itest-") { try await client.deleteTopic(topic.name) }
            for group in try await client.listGroups() where group.id.hasPrefix("km-itest-g-") {
                deleteTestGroup(bootstrap: Self.bootstrap, group: group.id)
            }
            return client
        }

        private func eventually(_ attempts: Int = 40, _ check: () async throws -> Bool) async throws -> Bool {
            for _ in 0..<attempts {
                if try await check() { return true }
                try await Task.sleep(for: .milliseconds(250))
            }
            return false
        }

        /// Topic with 3 partitions holding 20 / 15 / 10 messages, one second apart in event time, and an Empty group that
        /// committed 5 / 15 / 0. (An Empty group only lists partitions it has committed to.)
        private func fixture(_ client: LiveKafkaClient) async throws -> (topic: String, group: String, base: Int64) {
            let suffix = UUID().uuidString.prefix(8).lowercased()
            let topic = "km-itest-\(suffix)"
            let group = "km-itest-g-\(suffix)"
            try await client.createTopic(NewTopic(name: topic, partitions: 3, replicationFactor: 1))
            _ = try await eventually { (try? await client.describeTopic(topic))?.partitions.allSatisfy { $0.leader != nil } == true }
            let base = Int64(Date().timeIntervalSince1970 * 1000) - 600_000
            let counts = [20, 15, 10]
            try TestProducer(bootstrap: Self.bootstrap).produce(
                counts.enumerated().flatMap { partition, count in
                    (0..<count).map { TestMessage(partition: Int32(partition), key: nil, value: Data("p\(partition)-\($0)".utf8), timestampMs: base + Int64($0) * 1000) }
                },
                to: topic)
            var consumer: TestGroupConsumer? = try TestGroupConsumer(bootstrap: Self.bootstrap, group: group)
            try consumer?.join(topic: topic, expectingPartitions: 3)
            try consumer?.commit(topic: topic, offsets: [0: 5, 1: 15, 2: 0])
            consumer?.close()
            consumer = nil
            let emptied = try await eventually { try await client.describeGroup(group).state == "Empty" }
            #expect(emptied)
            return (topic, group, base)
        }

        private func cleanUp(_ client: LiveKafkaClient, topic: String, group: String) async {
            deleteTestGroup(bootstrap: Self.bootstrap, group: group)
            try? await client.deleteTopic(topic)
            await client.disconnect()
        }

        @Test func latestEarliestTimestampAndOffsetOnAnEmptyGroup() async throws {
            let client = try await connected()
            let (topic, group, base) = try await fixture(client)
            try requireTestOwnedGroup(group)

            // Latest: the lag disappears.
            let latest = try await client.previewOffsetReset(group: group, target: .latest, scope: .wholeGroup)
            #expect(latest.groupState == .empty)
            #expect(latest.rows.map(\.newOffset) == [20, 15, 10])
            #expect(latest.totalLagBefore == 15 + 0 + 10 && latest.totalLagAfter == 0)
            // A preview changes nothing.
            #expect(try await client.describeGroup(group).lags.map(\.committedOffset) == [5, 15, 0])

            // Earliest: lag becomes the retained range.
            let earliest = try await client.previewOffsetReset(group: group, target: .earliest, scope: .wholeGroup)
            #expect(earliest.rows.map(\.newOffset) == [0, 0, 0])
            #expect(earliest.totalLagAfter == 45)

            // Timestamp: first message at or after base + 7 s is offset 7 (partition 2 has 10 messages, so 7 as well).
            let timestamp = try await client.previewOffsetReset(group: group, target: .timestamp(Date(timeIntervalSince1970: Double(base + 7000) / 1000)), scope: .wholeGroup)
            #expect(timestamp.rows.map(\.newOffset) == [7, 7, 7])
            // After the last message: the end.
            let future = try await client.previewOffsetReset(group: group, target: .timestamp(Date(timeIntervalSince1970: Double(base + 3_600_000) / 1000)), scope: .wholeGroup)
            #expect(future.rows.map(\.newOffset) == [20, 15, 10])

            // Offset beyond the end is clamped.
            let beyond = try await client.previewOffsetReset(group: group, target: .offset(999), scope: .wholeGroup)
            #expect(beyond.rows.allSatisfy(\.isClamped) && beyond.rows.map(\.newOffset) == [20, 15, 10])

            // Apply earliest to one topic scope, verify, then latest for the whole group.
            let applied = try await client.resetOffsets(group: group, plan: earliest.limited(to: .partitions(topic: topic, [0])).plan)
            #expect(applied.applied == 1 && applied.isComplete)
            #expect(try await client.describeGroup(group).lags.map(\.committedOffset) == [0, 15, 0])

            let result = try await client.resetOffsets(group: group, plan: latest.plan)
            #expect(result.applied == 2 && result.isComplete)
            let after = try await client.describeGroup(group)
            #expect(after.lags.map(\.committedOffset) == [20, 15, 10])
            #expect(GroupLag.total(after.lags) == 0)

            await cleanUp(client, topic: topic, group: group)
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        @Test func aGroupWithAnActiveMemberIsRejected() async throws {
            let client = try await connected()
            let (topic, group, _) = try await fixture(client)
            try requireTestOwnedGroup(group)
            var consumer: TestGroupConsumer? = try TestGroupConsumer(bootstrap: Self.bootstrap, group: group)
            try consumer?.join(topic: topic, expectingPartitions: 3)

            let before = try await client.describeGroup(group)
            #expect(before.state == "Stable")
            let plan = OffsetResetPlan(assignments: [OffsetResetAssignment(topic: topic, partition: 0, offset: 20)])
            var failure: KafkaError?
            do { _ = try await client.resetOffsets(group: group, plan: plan) } catch let error as KafkaError { failure = error }
            #expect(failure?.isNonEmptyGroup == true, "\(String(describing: failure))")
            #expect(failure?.userMessage == OffsetResetRule.activeMembersMessage)
            #expect(try await client.describeGroup(group).lags.first?.committedOffset == before.lags.first?.committedOffset)

            consumer?.close()
            consumer = nil
            await cleanUp(client, topic: topic, group: group)
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        @Test func aMissingPartitionIsReportedWithoutBlockingTheRest() async throws {
            let client = try await connected()
            let (topic, group, _) = try await fixture(client)
            try requireTestOwnedGroup(group)
            let plan = OffsetResetPlan(assignments: [
                OffsetResetAssignment(topic: topic, partition: 0, offset: 20), OffsetResetAssignment(topic: topic, partition: 9, offset: 1),
            ])
            let result = try await client.resetOffsets(group: group, plan: plan)
            #expect(result.applied == 1)
            #expect(result.failures.map(\.partition) == [9])
            #expect(try await client.describeGroup(group).lags.first?.committedOffset == 20)
            await cleanUp(client, topic: topic, group: group)
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }
    }
}
