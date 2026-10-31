import Foundation
import Observation
import KafkaManagerCore

/// Builds the raw client for a profile. The session wraps whatever it returns in `GuardedKafkaClient`.
public typealias ClientFactory = @Sendable (ClusterConnection) -> any KafkaClient

public enum ClusterSessionError: Error, Sendable, Equatable {
    case notConnected

    public var userMessage: String {
        String(localized: "Not connected to the cluster.")
    }
}

/// One lazily-connected, read-only-aware, idle-closing connection to a cluster. Nothing happens at init: the client is
/// created by the first `connect()`, and the password is read from the secret store only then.
@MainActor @Observable
public final class ClusterSession {
    public enum Status: Sendable, Equatable {
        case idle
        case connecting
        case connected
        case failed(message: String)
    }

    public private(set) var status = Status.idle
    public private(set) var connection: ClusterConnection
    /// Applies to the current client and to the next one. Starts at the environment's default.
    public private(set) var isReadOnly: Bool

    @ObservationIgnored private let secrets: any SecretStore
    @ObservationIgnored private let factory: ClientFactory
    @ObservationIgnored private let idleTimeout: Duration
    @ObservationIgnored private let sleep: @Sendable (Duration) async throws -> Void

    @ObservationIgnored private var guarded: GuardedKafkaClient?
    @ObservationIgnored private var connectTask: Task<Void, Never>?
    @ObservationIgnored private var expiry: Task<Void, Never>?
    @ObservationIgnored private var lastFailure: (any Error)?
    /// Bumped by every disconnect, so a connect that finishes afterwards knows its result is stale.
    @ObservationIgnored private var generation = 0

    public init(
        connection: ClusterConnection,
        secrets: any SecretStore,
        factory: @escaping ClientFactory,
        idleTimeout: Duration = .seconds(300),
        sleep: @escaping @Sendable (Duration) async throws -> Void = { try await Task.sleep(for: $0) }
    ) {
        self.connection = connection
        self.secrets = secrets
        self.factory = factory
        self.idleTimeout = idleTimeout
        self.sleep = sleep
        self.isReadOnly = GuardedKafkaClient.defaultReadOnly(for: connection.environment)
    }

    // MARK: Client access

    /// The guarded client while connected; otherwise throws so the caller can `connect()` first.
    public func client() throws -> any KafkaClient {
        guard status == .connected, let guarded else { throw ClusterSessionError.notConnected }
        return guarded
    }

    /// Connects on demand and returns the client. Counts as activity. Concurrent callers share one connect.
    public func ensureConnected() async throws -> any KafkaClient {
        if status != .connected { await connect() }
        guard status == .connected, let guarded else { throw lastFailure ?? ClusterSessionError.notConnected }
        touch()
        return guarded
    }

    // MARK: Lifecycle

    /// Does nothing unless idle or failed; a call during an in-flight connect waits for it.
    public func connect() async {
        if let connectTask {
            await connectTask.value
            return
        }
        switch status {
        case .idle, .failed: break
        case .connecting, .connected: return
        }
        let task = Task { await performConnect() }
        connectTask = task
        await task.value
        connectTask = nil
    }

    public func disconnect() async {
        generation += 1
        expiry?.cancel()
        expiry = nil
        let client = guarded
        guarded = nil
        lastFailure = nil
        status = .idle
        await client?.disconnect()
    }

    /// Records activity: restarts the single idle timer. No-op unless connected.
    public func touch() {
        guard status == .connected else { return }
        expiry?.cancel()
        let timeout = idleTimeout
        let sleep = sleep
        expiry = Task { [weak self] in
            do { try await sleep(timeout) } catch { return }
            await self?.disconnect()
        }
    }

    /// Any change to the profile drops the connection; the next connect uses the new profile. A new environment
    /// also resets read-only to that environment's default.
    public func updateConnection(_ new: ClusterConnection) async {
        guard new.id == connection.id, new != connection else { return }
        if new.environment != connection.environment {
            isReadOnly = GuardedKafkaClient.defaultReadOnly(for: new.environment)
        }
        connection = new
        if status != .idle { await disconnect() }
    }

    public func setReadOnly(_ value: Bool) async {
        isReadOnly = value
        await guarded?.setReadOnly(value)
    }

    // MARK: Connect

    private func performConnect() async {
        status = .connecting
        lastFailure = nil
        let profile = connection
        let ticket = generation
        let client = GuardedKafkaClient(wrapping: factory(profile), clusterName: profile.name, isReadOnly: isReadOnly)
        guarded = client
        do {
            let credentials = try await credentials(for: profile)
            try await client.connect(profile, credentials: credentials)
        } catch {
            guard ticket == generation else { await client.disconnect(); return }
            guarded = nil
            lastFailure = error
            status = .failed(message: Self.message(for: error))
            await client.disconnect()
            return
        }
        guard ticket == generation else { await client.disconnect(); return }
        status = .connected
        touch()
    }

    /// The password lives only in this call's locals.
    private func credentials(for profile: ClusterConnection) async throws -> Credentials? {
        guard profile.security == .saslPlaintext || profile.security == .saslSsl, let username = profile.username else {
            return nil
        }
        let password = try await secrets.password(for: profile.id) ?? ""
        return Credentials(username: username, password: password)
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case let error as KafkaError: error.userMessage
        case let error as SecretStoreError: error.userMessage
        default: String(localized: "Couldn't connect to the cluster.")
        }
    }
}
