import Foundation
import KafkaManagerCore
import KafkaManagerUI

/// Composition root. Construction does no I/O beyond resolving the database path: the SQLite file is opened by the
/// first list load, and the Keychain is touched only when a form saves or checks for a stored password.
@MainActor
final class AppEnvironment {
    let connections: ConnectionsViewModel
    /// Empty until a feature asks for a cluster; nothing connects at launch.
    let sessions: ClusterSessions
    let shell: ShellViewModel

    init() {
        let store: any ConnectionStore
        do {
            store = SQLiteConnectionStore(location: .file(try Self.databaseURL()))
        } catch {
            store = UnavailableConnectionStore()
        }
        let secrets = KeychainSecretStore()
        let sessions = ClusterSessions(secrets: secrets, factory: DefaultClientFactory.demo)
        self.sessions = sessions
        connections = ConnectionsViewModel(
            store: store, secrets: secrets, observer: sessions,
            tester: ClientConnectionTester(factory: DefaultClientFactory.demo))
        shell = ShellViewModel(connections: connections)
    }
}

/// The one place that decides which Kafka client the app talks to.
enum DefaultClientFactory {
    static let defaultDemoTopicCount = 200
    private static let maxDemoTopicCount = 20_000

    /// `KAFKAMANAGER_DEMO_TOPICS=5000` makes the demo cluster that big, for manual performance checks (dev only).
    static let demoTopicCount: Int = {
        let text = ProcessInfo.processInfo.environment["KAFKAMANAGER_DEMO_TOPICS"] ?? ""
        guard let count = Int(text), (1...maxDemoTopicCount).contains(count) else { return defaultDemoTopicCount }
        return count
    }()

    private static let maxDemoMessagesPerPartition = 5_000

    /// `KAFKAMANAGER_DEMO_MESSAGES=3400` sets the messages per partition (default 30), for measuring big fetches.
    static let demoMessagesPerPartition: Int = {
        let text = ProcessInfo.processInfo.environment["KAFKAMANAGER_DEMO_MESSAGES"] ?? ""
        guard let count = Int(text), (1...maxDemoMessagesPerPartition).contains(count) else { return FakeKafkaClient.sampleMessagesPerPartition }
        return count
    }()

    /// Demo data for every profile. Replace with LiveKafkaClient in T60.
    static let demo: ClientFactory = { _ in
        FakeKafkaClient.sampleData(topicCount: demoTopicCount, messagesPerPartition: demoMessagesPerPartition)
    }
}

extension AppEnvironment {
    /// `KAFKAMANAGER_DATABASE` points the app at another file, so manual testing never touches real profiles.
    fileprivate static func databaseURL() throws -> URL {
        if let path = ProcessInfo.processInfo.environment["KAFKAMANAGER_DATABASE"], !path.isEmpty {
            return URL(fileURLWithPath: path)
        }
        return try SQLiteConnectionStore.defaultURL()
    }
}

/// Stands in when Application Support can't be resolved, so the list shows a retryable error instead of
/// silently keeping connections in memory only.
private struct UnavailableConnectionStore: ConnectionStore {
    func load() async throws -> [ClusterConnection] { throw Self.failure }
    func save(_ connection: ClusterConnection) async throws { throw Self.failure }
    func delete(id: UUID) async throws { throw Self.failure }

    private static let failure = ConnectionStoreError.database(code: -1, message: "Application Support is unavailable")
}
