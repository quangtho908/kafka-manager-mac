import Foundation
import Observation
import KafkaManagerCore

/// Learns about profile changes so open sessions stay in step with the saved list.
@MainActor
public protocol ConnectionLifecycleObserver: AnyObject {
    func connectionSaved(_ connection: ClusterConnection) async
    func connectionDeleted(id: UUID) async
}

/// Registry of sessions, one per cluster id, created on first use. Nothing is created or connected at launch.
@MainActor @Observable
public final class ClusterSessions {
    @ObservationIgnored private var sessions: [UUID: ClusterSession] = [:]
    @ObservationIgnored private let secrets: any SecretStore
    @ObservationIgnored private let factory: ClientFactory
    @ObservationIgnored private let idleTimeout: Duration
    @ObservationIgnored private let sleep: @Sendable (Duration) async throws -> Void

    public init(
        secrets: any SecretStore,
        factory: @escaping ClientFactory,
        idleTimeout: Duration = .seconds(300),
        sleep: @escaping @Sendable (Duration) async throws -> Void = { try await Task.sleep(for: $0) }
    ) {
        self.secrets = secrets
        self.factory = factory
        self.idleTimeout = idleTimeout
        self.sleep = sleep
    }

    public var openSessionCount: Int { sessions.count }

    /// The session for this profile, created if needed. If the profile changed since the session was made, the
    /// session is updated first (which drops its connection).
    public func session(for connection: ClusterConnection) async -> ClusterSession {
        if let existing = sessions[connection.id] {
            await existing.updateConnection(connection)
            return existing
        }
        let session = ClusterSession(connection: connection, secrets: secrets, factory: factory, idleTimeout: idleTimeout, sleep: sleep)
        sessions[connection.id] = session
        return session
    }

    /// Disconnects but keeps the session, so the next use reconnects lazily.
    public func close(id: UUID) async {
        await sessions[id]?.disconnect()
    }

    public func closeAll() async {
        for session in sessions.values { await session.disconnect() }
    }

    /// The profile was deleted: disconnect and forget it.
    public func remove(id: UUID) async {
        guard let session = sessions.removeValue(forKey: id) else { return }
        await session.disconnect()
    }
}

extension ClusterSessions: ConnectionLifecycleObserver {
    public func connectionSaved(_ connection: ClusterConnection) async {
        await sessions[connection.id]?.updateConnection(connection)
    }

    public func connectionDeleted(id: UUID) async {
        await remove(id: id)
    }
}
