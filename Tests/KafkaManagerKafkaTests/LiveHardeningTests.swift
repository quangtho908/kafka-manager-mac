import Foundation
import KafkaManagerCore
import KafkaManagerUI
import Testing
@testable import KafkaManagerKafka

/// Hardening against a real broker; skipped unless `KM_ITEST=1`. Only topics named `km-itest-*` and groups named
/// `km-itest-g-*` are created or deleted. The scale test (300 topics, 60 s idle) additionally needs `KM_ITEST_SCALE=1`.
extension LiveSerial {
    @Suite(.enabled(if: ProcessInfo.processInfo.environment["KM_ITEST"] == "1"))
    struct HardeningTests {
        private static let prefix = "km-itest-"
        private static let bootstrap = ProcessInfo.processInfo.environment["KM_ITEST_BOOTSTRAP"] ?? "localhost:9092"
        private let cluster = ClusterConnection(name: "itest", bootstrapServers: bootstrap)

        private static func uniqueName(_ tag: String = "") -> String { prefix + tag + UUID().uuidString.prefix(8).lowercased() }

        private func connected() async throws -> LiveKafkaClient {
            let client = LiveKafkaClient(timeout: .seconds(10))
            try await client.connect(cluster, credentials: nil)
            for topic in try await client.listTopics() where topic.name.hasPrefix(Self.prefix) {
                try await client.deleteTopic(topic.name)
            }
            return client
        }

        private func makeTopic(_ client: LiveKafkaClient, partitions: Int = 3) async throws -> String {
            let name = Self.uniqueName()
            try await client.createTopic(NewTopic(name: name, partitions: partitions, replicationFactor: 1))
            for _ in 0..<40 {
                if let detail = try? await client.describeTopic(name), detail.partitions.count == partitions,
                   detail.partitions.allSatisfy({ $0.leader != nil }) { break }
                try await Task.sleep(for: .milliseconds(250))
            }
            return name
        }

        /// Counters settle a moment after the last close (reader threads wind down); wait briefly instead of racing.
        private func countersDrain() async -> Bool {
            for _ in 0..<50 {
                if ResourceCounters.outstanding.isEmpty { return true }
                try? await Task.sleep(for: .milliseconds(100))
            }
            return false
        }

        // MARK: Scale

        @Test(.enabled(if: ProcessInfo.processInfo.environment["KM_ITEST_SCALE"] == "1"), .timeLimit(.minutes(6)))
        func threeHundredTopicsListAndIdle() async throws {
            let client = try await connected()
            let names = (0..<300).map { Self.prefix + String(format: "scale-%03d-", $0) + UUID().uuidString.prefix(4).lowercased() }
            defer { Task { for name in names { try? await client.deleteTopic(name) } } }

            let createTime = try await ContinuousClock().measure {
                try await withThrowingTaskGroup(of: Void.self) { group in
                    var iterator = names.makeIterator()
                    var running = 0
                    while let name = iterator.next() {
                        if running == 16 { try await group.next(); running -= 1 }
                        group.addTask { try await client.createTopic(NewTopic(name: name, partitions: 1, replicationFactor: 1)) }
                        running += 1
                    }
                    try await group.waitForAll()
                }
            }
            // Wait for all to appear.
            var count = 0
            for _ in 0..<60 {
                count = try await client.listTopics().filter { $0.name.hasPrefix(Self.prefix) }.count
                if count == names.count { break }
                try await Task.sleep(for: .milliseconds(500))
            }
            #expect(count == names.count)

            var times: [Duration] = []
            for _ in 0..<7 {
                let start = ContinuousClock.now
                _ = try await client.listTopics()
                times.append(ContinuousClock.now - start)
            }
            times.sort()
            let footprintBefore = ProcessMetrics.footprintBytes()
            var total = 0
            for _ in 0..<20 { total += try await client.listTopics().count }
            let footprintAfter = ProcessMetrics.footprintBytes()
            #expect(total > 0)

            // Idle while connected.
            let cpuStart = ProcessMetrics.cpuSeconds()
            let threads = ProcessMetrics.threadCount()
            try await Task.sleep(for: .seconds(60))
            let cpu = ProcessMetrics.cpuSeconds() - cpuStart
            Swift.print("""
            T61 SCALE (real broker, \(names.count) single-partition topics + \(count - names.count) other km-itest):
              create 300 (16 parallel): \(createTime)
              listTopics median \(times[3]), min \(times[0]), max \(times[6])
              footprint before 20 lists \(ProcessMetrics.megabytes(footprintBefore)), after \(ProcessMetrics.megabytes(footprintAfter))
              idle 60 s connected: \(String(format: "%.3f", cpu)) CPU s (\(String(format: "%.3f", cpu / 60 * 100)) %), threads \(threads)
            """)
            #expect(cpu < 3, "idle CPU \(cpu) s in 60 s")

            for name in names { try await client.deleteTopic(name) }
            await client.disconnect()
            #expect(await countersDrain(), "\(ResourceCounters.outstanding)")
        }

        // MARK: Stress

        private struct Tally: Sendable {
            var operations = 0
            var kafkaErrors = 0
            var unexpected: [String] = []
        }

        private actor Totals {
            var tally = Tally()
            func ok() { tally.operations += 1 }
            func kafkaError() { tally.kafkaErrors += 1 }
            func unexpected(_ text: String) { if tally.unexpected.count < 5 { tally.unexpected.append(text) } }
        }

        private static func record(_ totals: Totals, _ body: () async throws -> Void) async {
            do { try await body(); await totals.ok() } catch is KafkaError { await totals.kafkaError() } catch is CancellationError {
            } catch { await totals.unexpected("\(error)") }
        }

        @Test(.timeLimit(.minutes(3)))
        func oneClientUnderConcurrentLoadForTwentySeconds() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client)
            try TestProducer(bootstrap: Self.bootstrap).produce(
                (0..<300).map { TestMessage(partition: Int32($0 % 3), key: Data("k\($0)".utf8), value: Data("value-\($0)".utf8)) }, to: topic)
            let group = "km-itest-g-" + UUID().uuidString.prefix(8).lowercased()
            var member: TestGroupConsumer? = try TestGroupConsumer(bootstrap: Self.bootstrap, group: group)
            try member?.join(topic: topic, expectingPartitions: 3)
            try member?.commit(topic: topic, offsets: [0: 10, 1: 10, 2: 10])

            let threadsBefore = ProcessMetrics.threadCount()
            let footprintBefore = ProcessMetrics.footprintBytes()
            let deadline = ContinuousClock.now + .seconds(20)
            let totals = Totals()

            await withTaskGroup(of: Void.self) { tasks in
                tasks.addTask { while ContinuousClock.now < deadline { await Self.record(totals) { _ = try await client.listTopics() } } }
                tasks.addTask { while ContinuousClock.now < deadline { await Self.record(totals) { _ = try await client.describeTopic(topic) } } }
                tasks.addTask { while ContinuousClock.now < deadline { await Self.record(totals) { _ = try await client.describeGroup(group) } } }
                tasks.addTask { while ContinuousClock.now < deadline { await Self.record(totals) { _ = try await client.listGroups() } } }
                for reader in 0..<3 {
                    tasks.addTask {
                        var round = 0
                        while ContinuousClock.now < deadline {
                            round += 1
                            let query = MessageQuery(topic: topic, partition: reader == 2 ? nil : reader, start: .earliest, limit: 100)
                            await Self.record(totals) {
                                var seen = 0
                                for try await _ in client.fetchMessages(query) {
                                    seen += 1
                                    // Every third round walks away early.
                                    if round % 3 == 0, seen == 5 { break }
                                }
                            }
                        }
                    }
                }
                tasks.addTask {
                    while ContinuousClock.now < deadline {
                        let name = Self.uniqueName("s-")
                        await Self.record(totals) {
                            try await client.createTopic(NewTopic(name: name, partitions: 1, replicationFactor: 1))
                            try await client.deleteTopic(name)
                        }
                    }
                }
            }

            let tally = await totals.tally
            let threadsAfter = ProcessMetrics.threadCount()
            let footprintAfter = ProcessMetrics.footprintBytes()
            await client.disconnect()
            // The test's own consumer and producer are clients too.
            member?.close()
            member = nil
            deleteTestGroup(bootstrap: Self.bootstrap, group: group)
            let drained = await countersDrain()
            Swift.print("""
            T61 STRESS 20 s, one client, 8 concurrent loops: \(tally.operations) ok, \(tally.kafkaErrors) KafkaError, unexpected \(tally.unexpected)
              threads \(threadsBefore) -> \(threadsAfter) (client still open), footprint \(ProcessMetrics.megabytes(footprintBefore)) -> \(ProcessMetrics.megabytes(footprintAfter))
              threads after disconnect \(ProcessMetrics.threadCount()), counters empty: \(drained)
            """)
            #expect(tally.unexpected.isEmpty, "\(tally.unexpected)")
            #expect(tally.operations > 50)
            #expect(drained, "\(ResourceCounters.outstanding)")
            let cleanup = try await connected()
            await cleanup.disconnect()
        }

        @Test(.timeLimit(.minutes(3)))
        func connectDisconnectFiftyTimesLeavesNothingBehind() async throws {
            let threadsBefore = ProcessMetrics.threadCount()
            let shared = LiveKafkaClient(timeout: .seconds(10))
            for _ in 0..<50 {
                try await shared.connect(cluster, credentials: nil)
                #expect(try await shared.describeCluster().brokerCount >= 1)
                await shared.disconnect()
            }
            for _ in 0..<50 {
                let fresh = LiveKafkaClient(timeout: .seconds(10))
                try await fresh.connect(cluster, credentials: nil)
                _ = try await fresh.listTopics()
                await fresh.disconnect()
            }
            let drained = await countersDrain()
            try await Task.sleep(for: .seconds(1))
            let threadsAfter = ProcessMetrics.threadCount()
            Swift.print("T61 CYCLING 50 reuse + 50 fresh: threads \(threadsBefore) -> \(threadsAfter), counters empty \(drained)")
            #expect(drained, "\(ResourceCounters.outstanding)")
            #expect(threadsAfter <= threadsBefore + 2, "threads \(threadsBefore) -> \(threadsAfter)")
        }

        @Test(.timeLimit(.minutes(3)))
        func cancelStormOfFetches() async throws {
            let client = try await connected()
            let topic = try await makeTopic(client)
            try TestProducer(bootstrap: Self.bootstrap).produce(
                (0..<600).map { TestMessage(partition: Int32($0 % 3), key: nil, value: Data(repeating: 0x61, count: 2_000)) }, to: topic)
            let threadsBefore = ProcessMetrics.threadCount()
            var tasks: [Task<Int, Never>] = []
            for index in 0..<200 {
                tasks.append(Task {
                    var seen = 0
                    do { for try await _ in client.fetchMessages(MessageQuery(topic: topic, start: .earliest, limit: 600)) { seen += 1 } } catch {}
                    return seen
                })
                if index % 7 == 0 { try await Task.sleep(for: .milliseconds(3)) }
            }
            for (index, task) in tasks.enumerated() {
                if index % 5 != 0 { task.cancel() }
                if index % 11 == 0 { try await Task.sleep(for: .milliseconds(2)) }
            }
            var delivered = 0
            for task in tasks { delivered += await task.value }
            // The client must still answer after the storm.
            #expect(try await client.listTopics().contains { $0.name == topic })
            var rest = 0
            for try await _ in client.fetchMessages(MessageQuery(topic: topic, start: .earliest, limit: 600)) { rest += 1 }
            #expect(rest == 600)
            try await client.deleteTopic(topic)
            await client.disconnect()
            let drained = await countersDrain()
            let peak = ProcessMetrics.threadCount()
            // Dispatch worker threads that served the storm retire on their own; see how long that takes.
            let settleStart = ContinuousClock.now
            var settled = peak
            while ContinuousClock.now - settleStart < .seconds(40) {
                settled = ProcessMetrics.threadCount()
                if settled <= threadsBefore + 3 { break }
                try await Task.sleep(for: .seconds(1))
            }
            Swift.print("T61 CANCEL STORM 200 fetches (160 cancelled): delivered \(delivered) messages, follow-up read \(rest), counters empty \(drained), threads \(threadsBefore) -> \(peak) right after -> \(settled) after \(ContinuousClock.now - settleStart)")
            #expect(drained, "\(ResourceCounters.outstanding)")
            #expect(settled <= threadsBefore + 3, "threads stayed at \(settled), baseline \(threadsBefore)")
        }

        // MARK: Idle expiry

        private actor IdleFlag {
            private(set) var isSet = false
            func set() { isSet = true }
        }

        @Test(.timeLimit(.minutes(2)))
        @MainActor func idleExpiryDestroysTheLiveClient() async throws {
            try await Task.sleep(for: .seconds(3))
            let threadsBaseline = ProcessMetrics.threadCount()
            let footprintBaseline = ProcessMetrics.footprintBytes()
            let expired = IdleFlag()
            let session = ClusterSession(
                connection: cluster, secrets: InMemorySecretStore(),
                factory: { _ in LiveKafkaClient(timeout: .seconds(10)) },
                idleTimeout: .seconds(1),
                // Stands in for the clock: returns when the test says the idle time has passed.
                sleep: { _ in while !(await expired.isSet) { try await Task.sleep(for: .milliseconds(20)) } })
            let client = try await session.ensureConnected()
            _ = try await client.listTopics()
            #expect(session.status == .connected)
            #expect(ResourceCounters.live(.client) >= 1)
            let threadsConnected = ProcessMetrics.threadCount()
            let footprintConnected = ProcessMetrics.footprintBytes()

            await expired.set()
            for _ in 0..<100 where session.status != .idle { try await Task.sleep(for: .milliseconds(50)) }
            #expect(session.status == .idle)
            let drained = await countersDrain()
            try await Task.sleep(for: .seconds(1))
            let threadsAfter = ProcessMetrics.threadCount()
            let footprintAfter = ProcessMetrics.footprintBytes()
            Swift.print("""
            T61 IDLE EXPIRY (injected timeout): clients live \(ResourceCounters.live(.client)), counters empty \(drained)
              threads baseline \(threadsBaseline) -> connected \(threadsConnected) -> after expiry \(threadsAfter)
              footprint baseline \(ProcessMetrics.megabytes(footprintBaseline)) -> connected \(ProcessMetrics.megabytes(footprintConnected)) -> after \(ProcessMetrics.megabytes(footprintAfter))
            """)
            #expect(drained, "\(ResourceCounters.outstanding)")
            #expect(ResourceCounters.live(.client) == 0)
            #expect(threadsAfter < threadsConnected, "threads did not drop: \(threadsConnected) -> \(threadsAfter)")
            #expect(threadsAfter <= threadsBaseline + 1, "threads \(threadsBaseline) -> \(threadsAfter)")
            // A new request after expiry reconnects by itself.
            let again = try await session.ensureConnected()
            #expect(try await again.describeCluster().brokerCount >= 1)
            await session.disconnect()
        }

        // MARK: Error UX

        private struct Scenario {
            let name: String
            let connection: ClusterConnection
            let credentials: Credentials?
            let timeout: Duration
        }

        /// Text that must never reach the user: raw librdkafka wording, internals and secrets.
        private static let forbidden = [
            "librdkafka", "rdkafka", "Local:", "Broker:", "sasl_", "ssl://", "plaintext://", "GSSAPI", "errno", "getaddrinfo",
            "Connection refused", "Failed to resolve", "ECONNREFUSED", "hunter2-secret", "%", "(code -", "nodename",
        ]

        private func scenarios() -> [Scenario] {
            [
                Scenario(name: "unreachable host (closed port)", connection: ClusterConnection(name: "x", bootstrapServers: "127.0.0.1:1"), credentials: nil, timeout: .seconds(8)),
                Scenario(name: "DNS failure", connection: ClusterConnection(name: "x", bootstrapServers: "nonexistent.invalid:9092"), credentials: nil, timeout: .seconds(8)),
                Scenario(name: "wrong port", connection: ClusterConnection(name: "x", bootstrapServers: "localhost:9"), credentials: nil, timeout: .seconds(8)),
                Scenario(
                    name: "SASL on a plaintext listener",
                    connection: ClusterConnection(name: "x", bootstrapServers: Self.bootstrap, security: .saslPlaintext, saslMechanism: .plain, username: "alice"),
                    credentials: Credentials(username: "alice", password: "hunter2-secret"), timeout: .seconds(8)),
                Scenario(name: "timeout (blackhole address)", connection: ClusterConnection(name: "x", bootstrapServers: "10.255.255.1:9092"), credentials: nil, timeout: .seconds(3)),
            ]
        }

        @Test(.timeLimit(.minutes(3)))
        func realFailuresBecomeFriendlyMessages() async throws {
            var table: [String] = []
            for scenario in scenarios() {
                let client = LiveKafkaClient(timeout: scenario.timeout)
                let started = ContinuousClock.now
                var message = "<no error>"
                do {
                    try await client.connect(scenario.connection, credentials: scenario.credentials)
                    _ = try await client.describeCluster()
                } catch let error as KafkaError {
                    message = error.userMessage
                    for word in Self.forbidden { #expect(!message.contains(word), "\(scenario.name): '\(word)' in \(message)") }
                } catch {
                    Issue.record("\(scenario.name): not a KafkaError: \(error)")
                }
                await client.disconnect()
                #expect(message != "<no error>", "\(scenario.name) did not fail")
                table.append("  \(scenario.name) [\(ContinuousClock.now - started)] -> \(message)")
            }
            // Not found: a missing topic, and a missing group.
            let client = try await connected()
            do { _ = try await client.describeTopic(Self.uniqueName("missing-")); Issue.record("missing topic did not fail") } catch let error as KafkaError {
                table.append("  topic not found -> \(error.userMessage)")
                #expect(error.userMessage.contains("doesn't exist"))
            }
            do { _ = try await client.describeGroup("km-itest-g-missing-" + UUID().uuidString.prefix(6)); Issue.record("missing group did not fail") } catch let error as KafkaError {
                table.append("  group not found -> \(error.userMessage)")
                for word in Self.forbidden { #expect(!error.userMessage.contains(word), "group: '\(word)' in \(error.userMessage)") }
            }
            // A short deadline on a real call.
            let hurried = LiveKafkaClient(timeout: .milliseconds(1))
            do {
                try await hurried.connect(cluster, credentials: nil)
                _ = try await hurried.listTopics()
                table.append("  1 ms deadline -> (succeeded)")
            } catch let error as KafkaError {
                table.append("  1 ms deadline -> \(error.userMessage)")
                for word in Self.forbidden { #expect(!error.userMessage.contains(word), "deadline: '\(word)' in \(error.userMessage)") }
            }
            await hurried.disconnect()
            await client.disconnect()
            Swift.print("T61 ERROR UX (real scenarios -> userMessage):\n" + table.joined(separator: "\n"))
            #expect(await countersDrain(), "\(ResourceCounters.outstanding)")
        }

        @Test(.timeLimit(.minutes(2)))
        func connectionTesterWorksLive() async throws {
            let tester = ClientConnectionTester(factory: { _ in LiveKafkaClient(timeout: .seconds(10)) }, timeout: .seconds(15))
            let brokers = try await tester.test(cluster, credentials: nil)
            #expect(brokers == 1)

            var messages: [String] = []
            for scenario in scenarios() where scenario.name != "timeout (blackhole address)" {
                do { _ = try await tester.test(scenario.connection, credentials: scenario.credentials); Issue.record("\(scenario.name) passed") } catch let error as KafkaError {
                    messages.append("  \(scenario.name) -> \(error.userMessage)")
                    for word in Self.forbidden { #expect(!error.userMessage.contains(word), "\(scenario.name): '\(word)' in \(error.userMessage)") }
                }
            }
            // The tester's own 1 s deadline on an address that never answers.
            let quick = ClientConnectionTester(factory: { _ in LiveKafkaClient(timeout: .seconds(3)) }, timeout: .seconds(1))
            let started = ContinuousClock.now
            do { _ = try await quick.test(ClusterConnection(name: "x", bootstrapServers: "10.255.255.1:9092"), credentials: nil) } catch let error as KafkaError {
                messages.append("  tester deadline 1 s [\(ContinuousClock.now - started)] -> \(error.userMessage)")
                #expect(error == .timeout)
                // The tester waits for the client to give up, so it is bounded by the client's own timeout.
                #expect(ContinuousClock.now - started < .seconds(5))
            }
            Swift.print("T61 TESTER: live success, broker count \(brokers)\n" + messages.joined(separator: "\n"))
            #expect(await countersDrain(), "\(ResourceCounters.outstanding)")
        }
    }
}
