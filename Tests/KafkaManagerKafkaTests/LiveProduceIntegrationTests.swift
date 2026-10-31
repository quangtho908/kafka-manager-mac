import CRdKafka
import Foundation
import KafkaManagerCore
import Testing
@testable import KafkaManagerKafka

@Suite("Test-owned topic guard")
struct TestOwnedTopicGuardTests {
    @Test func onlyKmItestTopicsPassTheGuard() throws {
        try requireTestOwnedTopic("km-itest-abc123")
        for name in ["sync_offline_invoice", "orders", "km-itest-", "km-itest", "KM-ITEST-1", "", " km-itest-x", "x-km-itest-y"] {
            #expect(throws: (any Error).self) { try requireTestOwnedTopic(name) }
        }
    }

    @Test func theTestProducerRefusesOtherTopicsBeforeQueueingAnything() throws {
        let producer = try TestProducer(bootstrap: "127.0.0.1:1")
        #expect(throws: (any Error).self) { try producer.produce([TestMessage(partition: 0, key: nil, value: Data("x".utf8))], to: "orders") }
    }
}

/// Produces to a real broker; skipped unless `KM_ITEST=1`. Only `km-itest-<hex>` topics created by the test are ever
/// produced to, and they are deleted afterwards.
extension LiveSerial {
    @Suite(.enabled(if: ProcessInfo.processInfo.environment["KM_ITEST"] == "1"))
    struct ProduceIntegrationTests {
        private static let bootstrap = ProcessInfo.processInfo.environment["KM_ITEST_BOOTSTRAP"] ?? "localhost:9092"
        private let cluster = ClusterConnection(name: "itest", bootstrapServers: bootstrap)

        private func connected(producerIdle: Duration = LiveKafkaClient.defaultProducerIdle) async throws -> LiveKafkaClient {
            let client = LiveKafkaClient(timeout: .seconds(10), producerIdle: producerIdle)
            try await client.connect(cluster, credentials: nil)
            for topic in try await client.listTopics() where topic.name.hasPrefix("km-itest-") { try await client.deleteTopic(topic.name) }
            return client
        }

        private func makeTopic(_ client: LiveKafkaClient, partitions: Int = 3) async throws -> String {
            let topic = "km-itest-\(UUID().uuidString.prefix(8).lowercased())"
            try await client.createTopic(NewTopic(name: topic, partitions: partitions, replicationFactor: 1))
            for _ in 0..<40 {
                if (try? await client.describeTopic(topic))?.partitions.allSatisfy({ $0.leader != nil }) == true { break }
                try await Task.sleep(for: .milliseconds(250))
            }
            return topic
        }

        private func collect(_ client: LiveKafkaClient, _ query: MessageQuery) async throws -> [KafkaMessage] {
            var result: [KafkaMessage] = []
            for try await message in client.fetchMessages(query) { result.append(message) }
            return result
        }

        @Test func everyKindOfPayloadComesBackByteForByte() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client)
            let big = Data((0..<1_000_000).map { UInt8(truncatingIfNeeded: $0 &* 31) })
            let binary = Data([0x00, 0xFF, 0xFE, 0x80, 0x00, 0x7F, 0xC0])
            let cases: [(label: String, request: ProduceRequest)] = [
                ("text", ProduceRequest(topic: topic, partition: 0, key: Data("k1".utf8), value: Data("hello".utf8))),
                ("tombstone", ProduceRequest(topic: topic, partition: 0, key: Data("k2".utf8), value: nil)),
                ("empty value", ProduceRequest(topic: topic, partition: 0, key: nil, value: Data())),
                ("headers", ProduceRequest(topic: topic, partition: 0, key: nil, value: Data("h".utf8), headers: [
                    MessageHeader(key: "trace", value: Data("abc".utf8)), MessageHeader(key: "empty", value: Data()),
                    MessageHeader(key: "null", value: nil), MessageHeader(key: "bin", value: binary),
                ])),
                ("binary", ProduceRequest(topic: topic, partition: 0, key: binary, value: binary)),
                ("unicode", ProduceRequest(topic: topic, partition: 0, key: Data("ключ 🔑".utf8), value: Data("日本語 🎉 \u{0301}e".utf8))),
                ("1 MB", ProduceRequest(topic: topic, partition: 0, key: nil, value: big)),
            ]
            var results: [ProduceResult] = []
            for item in cases { results.append(try await client.produce(item.request)) }
            #expect(results.map(\.partition) == Array(repeating: 0, count: cases.count))
            #expect(results.map(\.offset) == Array(0..<Int64(cases.count)), "offsets come back in send order")

            let read = try await collect(client, MessageQuery(topic: topic, partition: 0, start: .earliest, limit: 100))
            #expect(read.count == cases.count)
            for (item, message) in zip(cases, read) {
                #expect(message.key == item.request.key, "\(item.label) key")
                #expect(message.value == item.request.value, "\(item.label) value")
                #expect(message.headers == item.request.headers, "\(item.label) headers")
            }
            #expect(read[1].value == nil && read[2].value == Data(), "a tombstone is not an empty value")

            // The returned timestamp is the stored one.
            #expect(abs(results[0].timestamp.timeIntervalSince(read[0].timestamp)) < 0.01)
            try await client.deleteTopic(topic)
            await client.disconnect()
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        @Test func autoPartitionAndExplicitTimestamp() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client)
            let when = Date(timeIntervalSince1970: 1_700_000_123.456)
            let keyed = try await client.produce(ProduceRequest(topic: topic, key: Data("same-key".utf8), value: Data("a".utf8), timestamp: when))
            let again = try await client.produce(ProduceRequest(topic: topic, key: Data("same-key".utf8), value: Data("b".utf8)))
            #expect(keyed.partition == again.partition, "the same key lands on the same partition")
            #expect(again.offset == keyed.offset + 1)
            #expect(abs(keyed.timestamp.timeIntervalSince(when)) < 0.002)
            let read = try await collect(client, MessageQuery(topic: topic, partition: keyed.partition, start: .earliest, limit: 10))
            #expect(read.first.map { abs($0.timestamp.timeIntervalSince(when)) < 0.002 } == true)
            #expect(Date().timeIntervalSince(again.timestamp) < 30, "no timestamp means now")
            var seen: Set<Int> = []
            for index in 0..<30 { seen.insert(try await client.produce(ProduceRequest(topic: topic, value: Data("n\(index)".utf8))).partition) }
            #expect(seen.count > 1, "keyless messages spread over partitions")
            for partition in seen { #expect((0..<3).contains(partition)) }
            await client.disconnect()
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        @Test func hundredSequentialProducesStayInOrder() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client, partitions: 1)
            var offsets: [Int64] = []
            let started = ContinuousClock.now
            for index in 0..<100 {
                offsets.append(try await client.produce(ProduceRequest(topic: topic, partition: 0, value: Data(String(format: "m%03d", index).utf8))).offset)
            }
            let elapsed = ContinuousClock.now - started
            FileHandle.standardError.write(Data("MEASURE 100 sequential produces \(elapsed)\n".utf8))
            #expect(offsets == Array(0..<100))
            let read = try await collect(client, MessageQuery(topic: topic, partition: 0, start: .earliest, limit: 200))
            #expect(read.compactMap { $0.value.flatMap { String(data: $0, encoding: .utf8) } } == (0..<100).map { String(format: "m%03d", $0) })
            await client.disconnect()
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        @Test func unknownTopicAndPartitionFailWithoutCreatingAnything() async throws {
            let client = try await connected()
            let missing = "km-itest-missing-\(UUID().uuidString.prefix(6).lowercased())"
            var failure: KafkaError?
            do { _ = try await client.produce(ProduceRequest(topic: missing, value: Data("x".utf8))) } catch let error as KafkaError { failure = error }
            #expect(failure == .unknownTopic(missing), "\(String(describing: failure))")
            // Neither the producer nor the metadata lookup created it.
            try await Task.sleep(for: .seconds(1))
            #expect(try await client.listTopics().contains { $0.name == missing } == false)

            let topic = try await makeTopic(client, partitions: 2)
            var badPartition: KafkaError?
            do { _ = try await client.produce(ProduceRequest(topic: topic, partition: 5, value: Data("x".utf8))) } catch let error as KafkaError { badPartition = error }
            #expect(badPartition?.userMessage.contains("no partition 5") == true, "\(String(describing: badPartition))")
            #expect(try await client.describeTopic(topic).partitions.allSatisfy { $0.endOffset == 0 }, "nothing was written")
            await client.disconnect()
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        @Test func tooLargeIsRefusedBeforeSendingAndInvalidRequestsToo() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client, partitions: 1)
            var failure: KafkaError?
            do {
                _ = try await client.produce(ProduceRequest(topic: topic, value: Data(count: ProduceValidator.defaultMaxMessageBytes + 1)))
            } catch let error as KafkaError { failure = error }
            #expect(failure?.userMessage == KafkaError.clusterError(code: KafkaError.messageTooLargeCode, message: "").userMessage)
            do { _ = try await client.produce(ProduceRequest(topic: topic, value: nil, headers: [MessageHeader(key: "", value: nil)])) } catch let error as KafkaError { failure = error }
            if case .clusterError(let code, _)? = failure { #expect(code == KafkaError.invalidProduceRequestCode) } else { Issue.record("expected clusterError") }
            #expect(try await client.describeTopic(topic).partitions[0].endOffset == 0)
            await client.disconnect()
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        @Test func producerHandleClosesWhenIdleAndOnDisconnect() async throws {
            let client = try await connected(producerIdle: .seconds(1))
            let topic = try await makeTopic(client, partitions: 1)
            let before = ResourceCounters.live(.client)
            _ = try await client.produce(ProduceRequest(topic: topic, partition: 0, value: Data("a".utf8)))
            #expect(ResourceCounters.live(.client) == before + 1, "one producer handle for the client, not one per message")
            _ = try await client.produce(ProduceRequest(topic: topic, partition: 0, value: Data("b".utf8)))
            #expect(ResourceCounters.live(.client) == before + 1)
            let threadsOpen = ProcessMetrics.threadCount()
            try await Task.sleep(for: .seconds(2.5))
            #expect(ResourceCounters.live(.client) == before, "closed after the idle time")
            let threadsIdle = ProcessMetrics.threadCount()
            FileHandle.standardError.write(Data("MEASURE threads with producer \(threadsOpen), after idle close \(threadsIdle)\n".utf8))
            #expect(threadsIdle < threadsOpen)
            // A later send opens a fresh one.
            _ = try await client.produce(ProduceRequest(topic: topic, partition: 0, value: Data("c".utf8)))
            #expect(ResourceCounters.live(.client) == before + 1)
            await client.disconnect()
            #expect(ResourceCounters.live(.client) == before - 1, "disconnect closes the session and the producer")
            #expect(try await collectAfterReconnect(topic: topic) == ["a", "b", "c"])
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }

        private func collectAfterReconnect(topic: String) async throws -> [String] {
            let reader = LiveKafkaClient(timeout: .seconds(10))
            try await reader.connect(cluster, credentials: nil)
            let read = try await collect(reader, MessageQuery(topic: topic, partition: 0, start: .earliest, limit: 10))
            try await reader.deleteTopic(topic)
            await reader.disconnect()
            return read.compactMap { $0.value.flatMap { String(data: $0, encoding: .utf8) } }
        }
    }
}

/// No broker needed: a producer pointed at a port nobody listens on.
extension LiveSerial {
    @Suite
    struct ProduceDeliveryFailureTests {
        @Test func anUnreachableBrokerEndsInAnErrorWithinTheTimeout() throws {
            let settings = ConsumerSettings(properties: [("bootstrap.servers", "127.0.0.1:1")], secrets: [], host: "127.0.0.1:1")
            let started = ContinuousClock.now
            do {
                let handle = try LiveKafkaClient.makeProducer(settings: settings, timeoutMs: 1500)
                defer { handle.drain() }
                _ = try LiveKafkaClient.deliver(
                    ProduceRequest(topic: "km-itest-unreachable", partition: 0, value: Data("x".utf8)),
                    on: handle, timeoutMs: 1500, host: "127.0.0.1:1", secrets: [])
                Issue.record("expected a failure")
            } catch let error as KafkaError {
                #expect(error == .timeout || error == .unreachable(host: "127.0.0.1:1"), "\(error)")
            }
            #expect(ContinuousClock.now - started < .seconds(6))
            #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
        }
    }
}
