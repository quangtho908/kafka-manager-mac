import Foundation
import KafkaManagerCore

struct SpyFailure: Error {}

/// Ordered record of calls across spies, to assert "secret first, then row".
actor EventLog {
    private(set) var events: [String] = []
    func record(_ event: String) { events.append(event) }
}

actor Gate {
    private var continuations: [CheckedContinuation<Void, Never>] = []
    private var isOpen = false

    func wait() async {
        if isOpen { return }
        await withCheckedContinuation { continuations.append($0) }
    }

    func open() {
        isOpen = true
        continuations.forEach { $0.resume() }
        continuations.removeAll()
    }
}

actor SpyConnectionStore: ConnectionStore {
    private let inner: InMemoryConnectionStore
    private let log: EventLog?
    private let gate: Gate?
    private(set) var callCount = 0
    var failSave = false
    var failDelete = false
    var failLoad = false
    var backup: URL?

    init(connections: [ClusterConnection] = [], log: EventLog? = nil, gate: Gate? = nil, backup: URL? = nil) {
        inner = InMemoryConnectionStore(connections: connections)
        self.log = log
        self.gate = gate
        self.backup = backup
    }

    func setFailures(save: Bool = false, delete: Bool = false, load: Bool = false) {
        failSave = save
        failDelete = delete
        failLoad = load
    }

    func load() async throws -> [ClusterConnection] {
        callCount += 1
        await log?.record("store.load")
        if let gate { await gate.wait() }
        if failLoad { throw ConnectionStoreError.database(code: 1, message: "SELECT secret-sql") }
        return try await inner.load()
    }

    func save(_ connection: ClusterConnection) async throws {
        callCount += 1
        await log?.record("store.save")
        if failSave { throw ConnectionStoreError.database(code: 1, message: "INSERT secret-sql") }
        try await inner.save(connection)
    }

    func delete(id: UUID) async throws {
        callCount += 1
        await log?.record("store.delete")
        if failDelete { throw ConnectionStoreError.database(code: 1, message: "DELETE secret-sql") }
        try await inner.delete(id: id)
    }

    func takeRecoveryBackup() async -> URL? {
        callCount += 1
        defer { backup = nil }
        return backup
    }
}

actor SpySecretStore: SecretStore {
    private let inner = InMemorySecretStore()
    private let log: EventLog?
    private(set) var callCount = 0
    var failSet = false
    var failDelete = false

    init(log: EventLog? = nil) { self.log = log }

    func setFailures(set: Bool = false, delete: Bool = false) {
        failSet = set
        failDelete = delete
    }

    func password(for id: UUID) async throws -> String? {
        callCount += 1
        return try await inner.password(for: id)
    }

    func setPassword(_ password: String, for id: UUID) async throws {
        callCount += 1
        await log?.record("secret.set")
        if failSet { throw SecretStoreError.keychain(status: -25293) }
        try await inner.setPassword(password, for: id)
    }

    func deletePassword(for id: UUID) async throws {
        callCount += 1
        await log?.record("secret.delete")
        if failDelete { throw SecretStoreError.keychain(status: -25293) }
        try await inner.deletePassword(for: id)
    }
}
