import Foundation
import KafkaManagerCore
import Testing
@testable import KafkaManagerKafka

/// Reads consumer groups from a real broker; skipped unless `KM_ITEST=1`. The only group ever joined, committed to or
/// deleted is a `km-itest-g-*` group the test creates itself. Other groups are only described (read-only).
extension LiveSerial {
    @Suite(.enabled(if: ProcessInfo.processInfo.environment["KM_ITEST"] == "1"))
    struct GroupsIntegrationTests {
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

        // MARK: A group that already exists (read only)

        @Test func describesAnExistingGroupReadOnly() async throws {
            let client = try await connected()
            let name = ProcessInfo.processInfo.environment["KM_ITEST_EXISTING_GROUP"] ?? "SyncInvoiceOfflineLocal"
            let before = try groupSnapshot(bootstrap: Self.bootstrap)
            guard try await client.listGroups().contains(where: { $0.id == name }) else {
                FileHandle.standardError.write(Data("MEASURE existing group \(name) not found; skipped\n".utf8))
                return
            }
            let detail = try await client.describeGroup(name)
            #expect(detail.state == "Empty")
            #expect(detail.members.isEmpty)
            #expect(!detail.lags.isEmpty)
            #expect(detail.lags.allSatisfy { ($0.committedOffset ?? 0) >= 0 && $0.endOffset >= $0.startOffset && $0.startOffset >= 0 })
            #expect(detail.coordinatorId != nil && detail.coordinatorHost != nil)
            #expect(try groupSnapshot(bootstrap: Self.bootstrap) == before, "describing changed nothing")
            await client.disconnect()
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        // MARK: A group the test owns

        @Test func activeThenEmptyGroupAndConcurrentDescribes() async throws {
            let client = try await connected()
            let suffix = UUID().uuidString.prefix(8).lowercased()
            let topic = "km-itest-\(suffix)"
            let group = "km-itest-g-\(suffix)"
            try await client.createTopic(NewTopic(name: topic, partitions: 3, replicationFactor: 1))
            defer {
                deleteTestGroup(bootstrap: Self.bootstrap, group: group)
                Task { try? await client.deleteTopic(topic) }
            }
            _ = try await eventually { (try? await client.describeTopic(topic))?.partitions.allSatisfy { $0.leader != nil } == true }
            let base = Int64(Date().timeIntervalSince1970 * 1000) - 60_000
            try TestProducer(bootstrap: Self.bootstrap).produce(
                (0..<10).map { TestMessage(partition: 0, key: nil, value: Data("a\($0)".utf8), timestampMs: base + Int64($0)) }
                    + (0..<10).map { TestMessage(partition: 1, key: nil, value: Data("b\($0)".utf8), timestampMs: base + Int64($0)) },
                to: topic)

            var consumer: TestGroupConsumer? = try TestGroupConsumer(bootstrap: Self.bootstrap, group: group)
            try consumer?.join(topic: topic, expectingPartitions: 3)
            // Partition 0 committed at 4 (lag 6), partition 1 never committed (lag = all 10), partition 2 committed at 0 (empty).
            try consumer?.commit(topic: topic, offsets: [0: 4, 2: 0])

            let detail = try await client.describeGroup(group)
            #expect(detail.state == "Stable")
            #expect(detail.members.count == 1)
            #expect(detail.members.first?.assignments[topic] == [0, 1, 2])
            #expect(detail.members.first?.id.isEmpty == false)
            #expect(detail.assignor == "range")
            #expect(detail.coordinatorId != nil)
            #expect(detail.coordinatorHost?.contains(":") == true)
            #expect(detail.lags.map(\.partition) == [0, 1, 2])
            #expect(detail.lags[0].committedOffset == 4 && detail.lags[0].startOffset == 0 && detail.lags[0].endOffset == 10 && detail.lags[0].lag == 6)
            #expect(detail.lags[1].committedOffset == nil && detail.lags[1].hasCommittedOffset == false && detail.lags[1].lag == 10)
            #expect(detail.lags[2].committedOffset == 0 && detail.lags[2].endOffset == 0 && detail.lags[2].lag == 0)
            #expect(GroupLag.total(detail.lags) == 16)

            let listed = try await client.listGroups().first { $0.id == group }
            #expect(listed?.state == "Stable")
            #expect(listed?.memberCount == 1)

            // Four describes in parallel, as the topic→groups scan does, 40 each: no crash, no leak, always the same answer.
            let started = ContinuousClock.now
            try await withThrowingTaskGroup(of: Int.self) { tasks in
                for _ in 0..<4 {
                    tasks.addTask {
                        var ok = 0
                        for _ in 0..<40 {
                            let again = try await client.describeGroup(group)
                            if again.lags.count == 3 && again.state == "Stable" { ok += 1 }
                        }
                        return ok
                    }
                }
                for try await ok in tasks { #expect(ok == 40) }
            }
            let parallel = ContinuousClock.now - started
            let singleStart = ContinuousClock.now
            for _ in 0..<20 { _ = try await client.describeGroup(group) }
            let single = (ContinuousClock.now - singleStart) / 20
            let listStart = ContinuousClock.now
            _ = try await client.listGroups()
            let list = ContinuousClock.now - listStart
            FileHandle.standardError.write(Data("MEASURE describeGroup avg \(single); 4×40 parallel total \(parallel); listGroups \(list)\n".utf8))

            consumer?.close()
            consumer = nil
            let emptied = try await eventually {
                let now = try await client.describeGroup(group)
                return now.state == "Empty" && now.members.isEmpty
            }
            #expect(emptied, "the group becomes Empty when its only member leaves")
            let after = try await client.describeGroup(group)
            // Without members only the partitions it committed to remain (partition 1 was never committed).
            #expect(after.lags.map(\.partition) == [0, 2], "an Empty group keeps showing lag for its commits")
            #expect(after.lags[0].lag == 6)

            deleteTestGroup(bootstrap: Self.bootstrap, group: group)
            let gone = try await eventually {
                do { _ = try await client.describeGroup(group); return false } catch let error as KafkaError { return error.isUnknownGroup }
            }
            #expect(gone, "a deleted group reads as unknown")
            await client.disconnect()
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        @Test func unknownGroupIsRecognised() async throws {
            let client = try await connected()
            var failure: KafkaError?
            do { _ = try await client.describeGroup("km-itest-g-does-not-exist") } catch let error as KafkaError { failure = error }
            #expect(failure?.isUnknownGroup == true, "\(String(describing: failure))")
            await client.disconnect()
        }
    }
}
