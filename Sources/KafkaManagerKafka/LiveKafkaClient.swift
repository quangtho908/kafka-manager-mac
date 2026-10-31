import CRdKafka
import Foundation
import KafkaManagerCore

/// `KafkaClient` on librdkafka. The handle is created on `connect` and destroyed on `disconnect`; nothing exists before.
///
/// Every blocking C call runs on this actor's own serial queue, never on the cooperative pool. The calls are bounded
/// by `timeout`, so a cancelled caller waits at most that long. Cancellation is checked between calls.
/// `fetchMessages`, `listGroups` and `describeGroup` arrive in T60b/T60c and fail with a placeholder error until then.
public actor LiveKafkaClient: KafkaClient {
    public static let defaultTimeout: Duration = .seconds(10)
    /// Code of the placeholder error returned by the operations that are not implemented yet.
    public static let notImplementedCode = -1

    private let queue = DispatchSerialQueue(label: "kafka-manager.live-client")
    nonisolated public var unownedExecutor: UnownedSerialExecutor { queue.asUnownedSerialExecutor() }

    let timeoutMs: Int32
    var session: Session?

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

    public init(timeout: Duration = LiveKafkaClient.defaultTimeout) {
        let seconds = timeout.components.seconds
        let millis = Int(seconds) * 1000 + Int(timeout.components.attoseconds / 1_000_000_000_000_000)
        timeoutMs = Int32(clamping: max(millis, 1))
    }

    // MARK: Connection

    public func connect(_ cluster: ClusterConnection, credentials: Credentials?) async throws {
        session = nil
        let settings = try ConnectionSettings(cluster: cluster, credentials: credentials)
        let sink = ErrorSink()
        let client = try Self.makeClient(settings: settings, sink: sink)
        let opened = Session(client: client, sink: sink, host: settings.primaryHost, secrets: settings.secrets)
        do {
            // One small request proves the brokers answer and that authentication works.
            _ = try RdMetadata(client: client, allTopics: false, topic: nil, timeoutMs: timeoutMs)
        } catch {
            throw map(error, session: opened, resource: settings.primaryHost)
        }
        session = opened
    }

    public func disconnect() async {
        session = nil
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
            for index in partitions.indices {
                try Task.checkCancellation()
                partitions[index] = try await watermarks(for: partitions[index], topic: name, session: session)
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

    // MARK: Not implemented yet (T60b / T60c)

    nonisolated public func fetchMessages(_ query: MessageQuery) -> AsyncThrowingStream<KafkaMessage, Error> {
        AsyncThrowingStream { $0.finish(throwing: Self.notImplemented) }
    }

    public func listGroups() async throws -> [GroupSummary] { throw Self.notImplemented }

    public func describeGroup(_ id: String) async throws -> GroupDetail { throw Self.notImplemented }

    static var notImplemented: KafkaError {
        .clusterError(code: notImplementedCode, message: "not implemented yet")
    }

    // MARK: Helpers

    private func requireSession() throws -> Session {
        guard let session else { throw KafkaError.unreachable(host: "the cluster") }
        return session
    }

    /// Turns a low-level failure into a `KafkaError`. A call that only timed out is explained by what the client's
    /// own threads reported meanwhile (all brokers down, bad credentials).
    func map(_ error: any Error, session: Session, resource: String) -> any Error {
        switch error {
        case let error as KafkaError: return error
        case is CancellationError: return error
        case RdFailure.code(let code):
            var effective = code
            if code == RD_KAFKA_RESP_ERR__TIMED_OUT.rawValue, let reported = session.sink.last { effective = reported }
            return LiveErrorMapping.map(code: effective, host: session.host, resource: resource, detail: nil, secrets: session.secrets)
        case RdFailure.config(let message):
            return LiveErrorMapping.map(code: -1, host: session.host, resource: resource, detail: message, secrets: session.secrets)
        default:
            return LiveErrorMapping.map(code: -1, host: session.host, resource: resource, detail: nil, secrets: session.secrets)
        }
    }

    private static func makeClient(settings: ConnectionSettings, sink: ErrorSink) throws -> RdClient {
        let conf = RdConf()
        for (key, value) in settings.properties {
            if let problem = conf.set(key, value) {
                let text = LiveErrorMapping.sanitize(problem, code: -1, secrets: settings.secrets)
                throw KafkaError.clusterError(code: -1, message: text)
            }
        }
        // librdkafka would otherwise log to stderr; the app has no use for that text and it could name hosts or users.
        conf.setLogCallback { _, _, _, _ in }
        conf.setErrorCallback { _, code, _, opaque in
            guard let opaque else { return }
            Unmanaged<ErrorSink>.fromOpaque(opaque).takeUnretainedValue().record(code: code)
        }
        do {
            return try RdClient(conf: conf, sink: sink)
        } catch RdFailure.config(let message) {
            throw KafkaError.clusterError(code: -1, message: LiveErrorMapping.sanitize(message, code: -1, secrets: settings.secrets))
        }
    }
}
