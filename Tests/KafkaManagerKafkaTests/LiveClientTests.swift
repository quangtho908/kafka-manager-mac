import CRdKafka
import KafkaManagerCore
import Testing
@testable import KafkaManagerKafka

extension LiveSerial {
@Suite struct ClientTests {
    private func conf(bootstrap: String = "localhost:1") -> RdConf {
        let conf = RdConf()
        #expect(conf.set("bootstrap.servers", bootstrap) == nil)
        return conf
    }

    @Test func handlesAreFreedExactlyOnce() throws {
        for _ in 0..<100 {
            _ = try RdClient(conf: conf(), sink: ErrorSink())
        }
        // A configuration that never became a client is still released.
        for _ in 0..<20 { _ = conf() }
        #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
    }

    @Test func rejectedPropertiesDoNotLeakTheConfiguration() {
        let bad = RdConf()
        #expect(bad.set("no.such.property", "x") != nil)
        #expect(bad.set("security.protocol", "NOT-A-PROTOCOL") != nil)
        _ = bad
    }

    @Test func adminObjectsQueuesAndEventsAreFreed() throws {
        let client = try RdClient(conf: conf(), sink: ErrorSink())
        for _ in 0..<5 {
            _ = try RdAdminOptions(client: client, operation: RD_KAFKA_ADMIN_OP_CREATETOPICS, timeoutMs: 200)
            let queue = try RdQueue(client: client)
            #expect(queue.poll(timeoutMs: 1) == nil)
            _ = try RdTopic(client: client, name: "unused")
        }
        #expect(ResourceCounters.live(.client) == 1)
        #expect(ResourceCounters.live(.queue) == 0)
        #expect(ResourceCounters.live(.adminOptions) == 0)
        #expect(ResourceCounters.live(.topic) == 0)
    }

    @Test func failedOperationsOnAnUnreachableClusterLeakNothing() async throws {
        let client = LiveKafkaClient(timeout: .milliseconds(300))
        let cluster = ClusterConnection(name: "dead", bootstrapServers: "localhost:1")
        for _ in 0..<5 {
            await #expect(throws: KafkaError.self) { try await client.connect(cluster, credentials: nil) }
        }
        await #expect(throws: KafkaError.self) { try await client.listTopics() }
        await client.disconnect()
        #expect(ResourceCounters.outstanding.isEmpty, "\(ResourceCounters.outstanding)")
    }

    @Test func unimplementedOperationsFailWithAPlaceholder() async throws {
        let client = LiveKafkaClient()
        await #expect(throws: LiveKafkaClient.notImplemented) { try await client.listGroups() }
        await #expect(throws: LiveKafkaClient.notImplemented) { try await client.describeGroup("g") }
    }

    @Test func fetchBeforeConnectFailsAsUnreachable() async {
        let client = LiveKafkaClient()
        var failure: (any Error)?
        do {
            for try await _ in client.fetchMessages(MessageQuery(topic: "t", start: .earliest, limit: 1)) {}
        } catch { failure = error }
        #expect(failure as? KafkaError == .unreachable(host: "the cluster"))
    }

    @Test func operationsBeforeConnectFailAsUnreachable() async {
        let client = LiveKafkaClient()
        await #expect(throws: KafkaError.self) { try await client.listTopics() }
        await #expect(throws: KafkaError.self) { try await client.describeCluster() }
    }
}
}
