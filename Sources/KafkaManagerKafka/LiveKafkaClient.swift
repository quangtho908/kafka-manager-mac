import CRdKafka
import Foundation
import KafkaManagerCore
import os

/// `KafkaClient` on librdkafka. The handle is created on `connect` and destroyed on `disconnect`; nothing exists before.
///
/// Every blocking C call runs on this actor's own serial queue, never on the cooperative pool. The calls are bounded
/// by `timeout`, so a cancelled caller waits at most that long. Cancellation is checked between calls.
public actor LiveKafkaClient: KafkaClient {
    public static let defaultTimeout: Duration = .seconds(10)

    private let queue = DispatchSerialQueue(label: "kafka-manager.live-client")
    nonisolated public var unownedExecutor: UnownedSerialExecutor { queue.asUnownedSerialExecutor() }

    let timeoutMs: Int32
    var session: Session?
    /// How long a producer handle stays open after its last send.
    let producerIdle: Duration
    /// Created on the first send, closed after `producerIdle` without one, and on disconnect.
    var producer: ProducerHandle?
    var producerUse = 0
    var producerIdleTask: Task<Void, Never>?
    /// What a fetch needs to open its own consumer; readable without the actor so `fetchMessages` can stay synchronous.
    nonisolated let consumerSettings = OSAllocatedUnfairLock(initialState: ConsumerSettings?.none)

    final class Session {
        let client: RdClient
        let sink: ErrorSink
        let host: String
        let secrets: [String]

        init(client: RdClient, sink: ErrorSink, host: String, secrets: [String]) {
            self.client = client
            self.sink = sink
            self.host = host
            self.secrets = secrets
        }
    }

    public static let defaultProducerIdle: Duration = .seconds(30)

    public init(timeout: Duration = LiveKafkaClient.defaultTimeout, producerIdle: Duration = LiveKafkaClient.defaultProducerIdle) {
        self.producerIdle = producerIdle
        let seconds = timeout.components.seconds
        let millis = Int(seconds) * 1000 + Int(timeout.components.attoseconds / 1_000_000_000_000_000)
        timeoutMs = Int32(clamping: max(millis, 1))
    }

    // MARK: Connection

    public func connect(_ cluster: ClusterConnection, credentials: Credentials?) async throws {
        closeProducer()
        session = nil
        consumerSettings.withLock { $0 = nil }
        let settings = try ConnectionSettings(cluster: cluster, credentials: credentials)
        let sink = ErrorSink()
        let client = try Self.makeClient(properties: settings.properties, secrets: settings.secrets, sink: sink)
        let opened = Session(client: client, sink: sink, host: settings.primaryHost, secrets: settings.secrets)
        do {
            // One small request proves the brokers answer and that authentication works.
            _ = try RdMetadata(client: client, allTopics: false, topic: nil, timeoutMs: timeoutMs)
        } catch {
            throw map(error, session: opened, resource: settings.primaryHost)
        }
        session = opened
        consumerSettings.withLock { $0 = ConsumerSettings(properties: settings.properties, secrets: settings.secrets, host: settings.primaryHost) }
    }

    public func disconnect() async {
        closeProducer()
        session = nil
        consumerSettings.withLock { $0 = nil }
    }

    public func describeCluster() async throws -> ClusterInfo {
        let session = try requireSession()
        do {
            let metadata = try RdMetadata(client: session.client, allTopics: false, topic: nil, timeoutMs: timeoutMs)
            return ClusterInfo(brokerCount: Int(metadata.pointer.pointee.broker_cnt))
        } catch {
            throw map(error, session: session, resource: session.host)
        }
    }

    // MARK: Topics

    public func listTopics() async throws -> [TopicSummary] {
        let session = try requireSession()
        do {
            let metadata = try RdMetadata(client: session.client, allTopics: true, topic: nil, timeoutMs: timeoutMs)
            return Self.topics(in: metadata)
        } catch {
            throw map(error, session: session, resource: session.host)
        }
    }

    public func describeTopic(_ name: String) async throws -> TopicDetail {
        let session = try requireSession()
        do {
            let topic = try RdTopic(client: session.client, name: name)
            let metadata = try RdMetadata(client: session.client, allTopics: false, topic: topic, timeoutMs: timeoutMs)
            var partitions = try Self.partitions(of: name, in: metadata)
            try Task.checkCancellation()
            // Leaderless partitions can't answer, so they keep 0/0 rather than failing the whole topic.
            let asked = partitions.filter { $0.leader != nil }.map { TopicPartition(topic: name, partition: $0.id) }
            let earliest = try await offsets(of: asked, spec: RD_KAFKA_OFFSET_SPEC_EARLIEST, session: session)
            let latest = try await offsets(of: asked, spec: RD_KAFKA_OFFSET_SPEC_LATEST, session: session)
            partitions = partitions.map { partition in
                let key = TopicPartition(topic: name, partition: partition.id)
                guard let low = earliest[key], let high = latest[key] else { return partition }
                return PartitionInfo(
                    id: partition.id, leader: partition.leader, replicas: partition.replicas, isr: partition.isr,
                    startOffset: low, endOffset: high)
            }
            let entries = try describeConfig(of: name, session: session)
            return TopicDetail(name: name, partitions: partitions, configEntries: entries)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw map(error, session: session, resource: name)
        }
    }

    public func createTopic(_ spec: NewTopic) async throws {
        let session = try requireSession()
        do {
            try createTopic(spec, session: session)
        } catch {
            throw map(error, session: session, resource: spec.name)
        }
    }

    public func deleteTopic(_ name: String) async throws {
        let session = try requireSession()
        do {
            try deleteTopic(name, session: session)
        } catch {
            throw map(error, session: session, resource: name)
        }
    }

    // MARK: Helpers

    func requireSession() throws -> Session {
        guard let session else { throw KafkaError.unreachable(host: "the cluster") }
        return session
    }

    /// Turns a low-level failure into a `KafkaError`. A call that only timed out is explained by what the client's
    /// own threads reported meanwhile (all brokers down, bad credentials).
    func map(_ error: any Error, session: Session, resource: String) -> any Error {
        Self.map(error, sink: session.sink, host: session.host, secrets: session.secrets, resource: resource)
    }

    static func map(_ error: any Error, sink: ErrorSink, host: String, secrets: [String], resource: String) -> any Error {
        switch error {
        case let error as KafkaError: return error
        case is CancellationError: return error
        case RdFailure.code(let code):
            var effective = code
            if code == RD_KAFKA_RESP_ERR__TIMED_OUT.rawValue, let reported = sink.last { effective = reported }
            return LiveErrorMapping.map(code: effective, host: host, resource: resource, detail: nil, secrets: secrets)
        case RdFailure.config(let message):
            return LiveErrorMapping.map(code: -1, host: host, resource: resource, detail: message, secrets: secrets)
        default:
            return LiveErrorMapping.map(code: -1, host: host, resource: resource, detail: nil, secrets: secrets)
        }
    }

    static func makeClient(
        properties: [(key: String, value: String)], secrets: [String], sink: ErrorSink,
        type: rd_kafka_type_t = RD_KAFKA_PRODUCER, configure: ((RdConf) -> Void)? = nil
    ) throws -> RdClient {
        let conf = RdConf()
        for (key, value) in properties {
            if let problem = conf.set(key, value) {
                let text = LiveErrorMapping.sanitize(problem, code: -1, secrets: secrets)
                throw KafkaError.clusterError(code: -1, message: text)
            }
        }
        // librdkafka would otherwise log to stderr; the app has no use for that text and it could name hosts or users.
        conf.setLogCallback { _, _, _, _ in }
        conf.setErrorCallback { _, code, _, opaque in
            guard let opaque else { return }
            Unmanaged<ErrorSink>.fromOpaque(opaque).takeUnretainedValue().record(code: code)
        }
        configure?(conf)
        do {
            return try RdClient(conf: conf, sink: sink, type: type)
        } catch RdFailure.config(let message) {
            throw KafkaError.clusterError(code: -1, message: LiveErrorMapping.sanitize(message, code: -1, secrets: secrets))
        }
    }
}
