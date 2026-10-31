import Foundation
import KafkaManagerCore
import Testing
@testable import KafkaManagerKafka

/// Reads from a real broker; skipped unless `KM_ITEST=1`. Writes only to topics named `km-itest-*` that it creates and
/// deletes itself.
extension LiveSerial {
    @Suite(.enabled(if: ProcessInfo.processInfo.environment["KM_ITEST"] == "1"))
    struct MessagesIntegrationTests {
        private static let bootstrap = ProcessInfo.processInfo.environment["KM_ITEST_BOOTSTRAP"] ?? "localhost:9092"
        // Recent timestamps: the broker would delete segments whose newest message is older than the retention.
        private static let baseMs = Int64(Date().timeIntervalSince1970 * 1000) - 600_000
        private let cluster = ClusterConnection(name: "itest", bootstrapServers: bootstrap)

        private struct Fixture {
            let client: LiveKafkaClient
            let topic: String
        }

        private func connected() async throws -> LiveKafkaClient {
            let client = LiveKafkaClient(timeout: .seconds(10))
            try await client.connect(cluster, credentials: nil)
            for topic in try await client.listTopics() where topic.name.hasPrefix("km-itest-") {
                try await client.deleteTopic(topic.name)
            }
            return client
        }

        private func makeTopic(_ client: LiveKafkaClient, partitions: Int = 3) async throws -> String {
            let name = "km-itest-" + UUID().uuidString.prefix(8).lowercased()
            // Room for the 1.5 MB message.
            try await client.createTopic(NewTopic(name: name, partitions: partitions, replicationFactor: 1, config: ["max.message.bytes": "2097152"]))
            for _ in 0..<40 {
                if let detail = try? await client.describeTopic(name), detail.partitions.count == partitions,
                   detail.partitions.allSatisfy({ $0.leader != nil }) { break }
                try await Task.sleep(for: .milliseconds(250))
            }
            return name
        }

        private func collect(_ client: LiveKafkaClient, _ query: MessageQuery) async throws -> [KafkaMessage] {
            var result: [KafkaMessage] = []
            for try await message in client.fetchMessages(query) { result.append(message) }
            return result
        }

        private func plain(_ partition: Int32, count: Int, timeBase: Int64) -> [TestMessage] {
            (0..<count).map { index in
                TestMessage(
                    partition: partition, key: Data("k-\(partition)-\(index)".utf8), value: Data("v-\(partition)-\(index)".utf8),
                    timestampMs: Self.baseMs + timeBase + Int64(index) * 1000)
            }
        }

        // MARK: Content

        @Test func contentIsReadFaithfully() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client)
            defer { Task { try? await client.deleteTopic(topic) } }
            let big = Data((0..<1_500_000).map { UInt8(truncatingIfNeeded: $0 &* 31) })
            let unicode = "héllo ✓ 日本語 🎉"
            let binary = Data([0x00, 0xFF, 0x10, 0x80, 0xC0, 0x7F])
            try TestProducer(bootstrap: Self.bootstrap).produce([
                TestMessage(partition: 0, key: Data("tomb".utf8), value: nil, timestampMs: Self.baseMs),
                TestMessage(partition: 0, key: nil, value: Data(), timestampMs: Self.baseMs + 1000),
                TestMessage(partition: 0, key: Data(), value: Data(unicode.utf8), headers: [
                    ("nil-header", nil), ("empty-header", Data()), ("text", Data("héllo".utf8)), ("text", Data([1, 2, 3])),
                ], timestampMs: Self.baseMs + 2000),
                TestMessage(partition: 0, key: binary, value: binary, timestampMs: Self.baseMs + 3000),
                TestMessage(partition: 1, key: Data("big".utf8), value: big, timestampMs: Self.baseMs + 4000),
            ], to: topic)

            let zero = try await collect(client, MessageQuery(topic: topic, partition: 0, start: .earliest, limit: 100))
            #expect(zero.map(\.offset) == [0, 1, 2, 3])
            #expect(zero[0].key == Data("tomb".utf8))
            #expect(zero[0].value == nil, "a tombstone has no value")
            #expect(zero[1].key == nil)
            #expect(zero[1].value == Data(), "an empty value is not a tombstone")
            #expect(zero[2].key == Data(), "an empty key is not an absent key")
            #expect(zero[2].value == Data(unicode.utf8))
            #expect(zero[2].headers == [
                MessageHeader(key: "nil-header", value: nil), MessageHeader(key: "empty-header", value: Data()),
                MessageHeader(key: "text", value: Data("héllo".utf8)), MessageHeader(key: "text", value: Data([1, 2, 3])),
            ])
            #expect(zero[3].key == binary && zero[3].value == binary)
            #expect(zero.map { $0.timestamp.timeIntervalSince1970 } == (0..<4).map { Double(Self.baseMs + Int64($0) * 1000) / 1000 })
            #expect(zero.allSatisfy { $0.timestampType == .createTime })

            let large = try await collect(client, MessageQuery(topic: topic, partition: 1, start: .earliest, limit: 10))
            #expect(large.count == 1)
            #expect(large.first?.value == big, "a 1.5 MB message arrives intact")
            await client.disconnect()
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        // MARK: Start positions, limits, ends

        @Test func startPositionsLimitsAndEnds() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client)
            defer { Task { try? await client.deleteTopic(topic) } }
            // Partition 0: 20 messages at +0 s…+19 s; partition 1: 20 at +100 s…; partition 2 stays empty.
            try TestProducer(bootstrap: Self.bootstrap).produce(plain(0, count: 20, timeBase: 0) + plain(1, count: 20, timeBase: 100_000), to: topic)

            let all = try await collect(client, MessageQuery(topic: topic, start: .earliest, limit: 1000))
            #expect(all.count == 40)
            for partition in [0, 1] {
                #expect(all.filter { $0.partition == partition }.map(\.offset) == Array(0..<20), "per-partition order")
            }

            #expect(try await collect(client, MessageQuery(topic: topic, start: .earliest, limit: 7)).count == 7)
            #expect(try await collect(client, MessageQuery(topic: topic, partition: 2, start: .earliest, limit: 7)).isEmpty, "empty partition ends at once")

            let tail = try await collect(client, MessageQuery(topic: topic, partition: 0, start: .latest(3), limit: 100))
            #expect(tail.map(\.offset) == [17, 18, 19])
            let newest = try await collect(client, MessageQuery(topic: topic, start: .latest(5), limit: 100))
            #expect(newest.count == 5)
            #expect(newest.allSatisfy { $0.partition == 1 && $0.offset >= 15 }, "the 5 newest by timestamp are all in partition 1")
            #expect(try await collect(client, MessageQuery(topic: topic, start: .latest(5), limit: 2)).count == 2, "limit bounds latest(N) too")
            #expect(try await collect(client, MessageQuery(topic: topic, partition: 0, start: .latest(500), limit: 1000)).count == 20)

            let from = try await collect(client, MessageQuery(topic: topic, partition: 0, start: .offset(10), limit: 100))
            #expect(from.map(\.offset) == Array(10..<20))
            #expect(try await collect(client, MessageQuery(topic: topic, partition: 0, start: .offset(10_000), limit: 100)).isEmpty, "past the end clamps to the end")
            #expect(try await collect(client, MessageQuery(topic: topic, partition: 0, start: .offset(-5), limit: 100)).count == 20, "before the start clamps to the start")

            let at = Date(timeIntervalSince1970: Double(Self.baseMs + 5000) / 1000)
            let byTime = try await collect(client, MessageQuery(topic: topic, partition: 0, start: .timestamp(at), limit: 100))
            #expect(byTime.map(\.offset) == Array(5..<20))
            let after = Date(timeIntervalSince1970: Double(Self.baseMs + 10_000_000) / 1000)
            #expect(try await collect(client, MessageQuery(topic: topic, start: .timestamp(after), limit: 100)).isEmpty, "no match reads nothing")

            await #expect(throws: KafkaError.unknownTopic("km-itest-missing")) {
                _ = try await collect(client, MessageQuery(topic: "km-itest-missing", start: .earliest, limit: 5))
            }
            var failure: KafkaError?
            do { _ = try await collect(client, MessageQuery(topic: topic, partition: 9, start: .earliest, limit: 5)) } catch let error as KafkaError { failure = error }
            guard case .clusterError = failure else {
                Issue.record("expected clusterError for an unknown partition, got \(String(describing: failure))")
                return
            }
            await client.disconnect()
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        @Test func emptyTopicEndsWithoutWaiting() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client)
            defer { Task { try? await client.deleteTopic(topic) } }
            let started = ContinuousClock.now
            #expect(try await collect(client, MessageQuery(topic: topic, start: .earliest, limit: 100)).isEmpty)
            #expect(ContinuousClock.now - started < .seconds(2))
            await client.disconnect()
        }

        // MARK: Cancel, backpressure, no offset impact

        @Test func cancellingStopsPromptlyAndFreesEverything() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client, partitions: 1)
            defer { Task { try? await client.deleteTopic(topic) } }
            try TestProducer(bootstrap: Self.bootstrap).produce(plain(0, count: 20_000, timeBase: 0), to: topic)

            let baseline = ResourceCounters.live(.client)
            var received = 0
            var cancelledAt = ContinuousClock.now
            for try await _ in client.fetchMessages(MessageQuery(topic: topic, start: .earliest, limit: 20_000)) {
                received += 1
                if received == 10 { cancelledAt = .now; break }
            }
            var freedAfter = ContinuousClock.now - cancelledAt
            while ResourceCounters.live(.client) > baseline, ContinuousClock.now - cancelledAt < .seconds(3) {
                try await Task.sleep(for: .milliseconds(5))
                freedAfter = ContinuousClock.now - cancelledAt
            }
            FileHandle.standardError.write(Data("MEASURE cancel→consumer destroyed: \(freedAfter)\n".utf8))
            #expect(ResourceCounters.live(.client) == baseline, "the consumer is destroyed")
            #expect(freedAfter < .milliseconds(300))
            await client.disconnect()
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        @Test func aSlowConsumerDoesNotLetTheReaderRunAhead() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client, partitions: 1)
            defer { Task { try? await client.deleteTopic(topic) } }
            try TestProducer(bootstrap: Self.bootstrap).produce(plain(0, count: 5000, timeBase: 0), to: topic)

            let before = FetchStats.enqueued.withLock { $0 }
            var received = 0
            for try await _ in client.fetchMessages(MessageQuery(topic: topic, start: .earliest, limit: 5000)) {
                received += 1
                if received == 1 {
                    try await Task.sleep(for: .seconds(1))
                    let ahead = FetchStats.enqueued.withLock { $0 } - before - received
                    #expect(ahead <= LiveKafkaClient.streamBuffer + 1, "buffered \(ahead) while the consumer slept")
                    break
                }
            }
            #expect(received == 1)
            await client.disconnect()
        }

        @Test func readingChangesNoConsumerGroup() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client)
            defer { Task { try? await client.deleteTopic(topic) } }
            try TestProducer(bootstrap: Self.bootstrap).produce(plain(0, count: 50, timeBase: 0), to: topic)
            let before = try groupSnapshot(bootstrap: Self.bootstrap)
            for start in [StartPosition.earliest, .latest(10), .offset(5)] {
                _ = try await collect(client, MessageQuery(topic: topic, start: start, limit: 100))
            }
            let after = try groupSnapshot(bootstrap: Self.bootstrap)
            #expect(before == after)
            #expect(!after.contains { $0.hasPrefix("km-noop-") })
            await client.disconnect()
        }

        // MARK: Numbers (printed to stderr, asserted loosely)

        @Test func speedAndMemory() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client, partitions: 1)
            defer { Task { try? await client.deleteTopic(topic) } }
            let payload = Data(repeating: 0x61, count: 2048)
            try TestProducer(bootstrap: Self.bootstrap).produce((0..<10_000).map {
                TestMessage(partition: 0, key: Data("k\($0)".utf8), value: payload, timestampMs: Self.baseMs + Int64($0))
            }, to: topic)

            let start = ContinuousClock.now
            let first = try await collect(client, MessageQuery(topic: topic, start: .earliest, limit: 500))
            let elapsed = ContinuousClock.now - start
            FileHandle.standardError.write(Data("MEASURE 500×2KB: \(elapsed)\n".utf8))
            #expect(first.count == 500)
            #expect(elapsed < .seconds(2))

            let footprintBefore = Self.footprintMB()
            for _ in 0..<3 {
                let all = try await collect(client, MessageQuery(topic: topic, start: .earliest, limit: 10_000))
                #expect(all.count == 10_000)
            }
            let footprintAfter = Self.footprintMB()
            FileHandle.standardError.write(Data("MEASURE footprint before \(footprintBefore) MB, after 3×10 000×2KB \(footprintAfter) MB\n".utf8))
            #expect(footprintAfter - footprintBefore < 60)
            await client.disconnect()
        }

        @Test func idleAfterFetchingCostsNoCPU() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client, partitions: 1)
            defer { Task { try? await client.deleteTopic(topic) } }
            try TestProducer(bootstrap: Self.bootstrap).produce(plain(0, count: 1000, timeBase: 0), to: topic)
            _ = try await collect(client, MessageQuery(topic: topic, start: .earliest, limit: 1000))
            let before = Self.cpuSeconds()
            try await Task.sleep(for: .seconds(15))
            let used = Self.cpuSeconds() - before
            FileHandle.standardError.write(Data("MEASURE idle CPU after a fetch (client still connected): \(used) s over 15 s (\(used / 15 * 100) %)\n".utf8))
            #expect(used < 0.3)
            await client.disconnect()
        }

        /// Opt-in: `KM_ITEST_READ_TOPIC=<existing topic>` reads (never writes) up to 50 messages and reports only counts.
        @Test(.enabled(if: ProcessInfo.processInfo.environment["KM_ITEST_READ_TOPIC"] != nil))
        func readsAnExistingTopicReadOnly() async throws {
            let client = LiveKafkaClient()
            try await client.connect(cluster, credentials: nil)
            let topic = ProcessInfo.processInfo.environment["KM_ITEST_READ_TOPIC"] ?? ""
            let before = try groupSnapshot(bootstrap: Self.bootstrap)
            let messages = try await collect(client, MessageQuery(topic: topic, start: .latest(50), limit: 50))
            let after = try groupSnapshot(bootstrap: Self.bootstrap)
            let kinds = Set(messages.compactMap(\.timestampType?.rawValue))
            FileHandle.standardError.write(Data("MEASURE read \(messages.count) messages from an existing topic; timestamp types \(kinds); sizes \(messages.prefix(3).map { $0.value?.count ?? -1 }); groups unchanged: \(before == after)\n".utf8))
            #expect(before == after)
            await client.disconnect()
        }

        private static func cpuSeconds() -> Double {
            var usage = rusage()
            getrusage(RUSAGE_SELF, &usage)
            func seconds(_ t: timeval) -> Double { Double(t.tv_sec) + Double(t.tv_usec) / 1e6 }
            return seconds(usage.ru_utime) + seconds(usage.ru_stime)
        }

        private static func footprintMB() -> Double {
            var info = task_vm_info_data_t()
            var count = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<integer_t>.size)
            let result = withUnsafeMutablePointer(to: &info) {
                $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) { task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &count) }
            }
            return result == KERN_SUCCESS ? Double(info.phys_footprint) / 1_048_576 : -1
        }
    }
}
