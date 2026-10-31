import Testing
import Foundation
import os
@testable import KafkaManagerCore

private final class TestClock: Sendable {
    private let current = OSAllocatedUnfairLock(initialState: Date(timeIntervalSince1970: 1_000))
    var now: Date { current.withLock { $0 } }
    func advance(by seconds: TimeInterval) { current.withLock { $0 = $0.addingTimeInterval(seconds) } }
}

private func temporaryDirectory() throws -> URL {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent("km-tests-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    return directory
}

private func connection(
    _ name: String,
    id: UUID = UUID(),
    servers: String = "localhost:9092",
    environment: ClusterConnection.Environment = .local,
    security: ClusterConnection.Security = .plaintext,
    mechanism: ClusterConnection.SASLMechanism? = nil,
    username: String? = nil
) -> ClusterConnection {
    ClusterConnection(id: id, name: name, bootstrapServers: servers, environment: environment, security: security, saslMechanism: mechanism, username: username)
}

private func timestamps(file: URL, id: UUID) throws -> (created: Double, updated: Double)? {
    let database = try SQLiteDatabase(path: file.path)
    return try database.query("SELECT created_at, updated_at FROM connections WHERE id = ?1", [.text(id.uuidString)]) { ($0.real(0), $0.real(1)) }.first
}

// MARK: Round trip, ordering, upsert, delete

@Test(arguments: [false, true])
func roundTripsAllFields(inMemory: Bool) async throws {
    let directory = try temporaryDirectory()
    let store = SQLiteConnectionStore(location: inMemory ? .memory : .file(directory.appendingPathComponent("db.sqlite")))
    let full = connection("prod", servers: "a:9093,b:9093", environment: .production, security: .saslSsl, mechanism: .scramSha512, username: "alice")
    let minimal = connection("local")
    try await store.save(full)
    try await store.save(minimal)
    let loaded = try await store.load()
    #expect(loaded == [full, minimal])
    #expect(loaded[1].saslMechanism == nil && loaded[1].username == nil)
}

@Test func loadOnEmptyStoreReturnsEmptyList() async throws {
    #expect(try await SQLiteConnectionStore(location: .memory).load().isEmpty)
}

@Test func loadsInInsertionOrderAndUpdateKeepsPosition() async throws {
    let store = SQLiteConnectionStore(location: .memory)
    let (a, b, c) = (connection("a"), connection("b"), connection("c"))
    for item in [b, a, c] { try await store.save(item) }
    #expect(try await store.load().map(\.name) == ["b", "a", "c"])
    var renamed = b
    renamed.name = "b2"
    try await store.save(renamed)
    #expect(try await store.load().map(\.name) == ["b2", "a", "c"])
}

@Test func ordersBySortOrderThenCreatedAt() async throws {
    let directory = try temporaryDirectory()
    let file = directory.appendingPathComponent("db.sqlite")
    let clock = TestClock()
    let store = SQLiteConnectionStore(location: .file(file), now: { clock.now })
    let (first, second, third) = (connection("first"), connection("second"), connection("third"))
    for item in [first, second, third] {
        try await store.save(item)
        clock.advance(by: 10)
    }
    // Same sort_order for all: created_at decides. Then an explicit sort_order wins over created_at.
    let database = try SQLiteDatabase(path: file.path)
    try database.execute("UPDATE connections SET sort_order = 0")
    try database.run("UPDATE connections SET created_at = 5 WHERE id = ?1", [.text(third.id.uuidString)])
    #expect(try await store.load().map(\.name) == ["third", "first", "second"])
    try database.run("UPDATE connections SET sort_order = 9 WHERE id = ?1", [.text(third.id.uuidString)])
    #expect(try await store.load().map(\.name) == ["first", "second", "third"])
}

@Test func upsertKeepsCreatedAtAndBumpsUpdatedAt() async throws {
    let directory = try temporaryDirectory()
    let file = directory.appendingPathComponent("db.sqlite")
    let clock = TestClock()
    let store = SQLiteConnectionStore(location: .file(file), now: { clock.now })
    var item = connection("one")
    try await store.save(item)
    let created = clock.now.timeIntervalSince1970
    clock.advance(by: 60)
    item.name = "renamed"
    item.environment = .staging
    try await store.save(item)
    let stamps = try #require(try timestamps(file: file, id: item.id))
    #expect(stamps.created == created)
    #expect(stamps.updated == created + 60)
    #expect(try await store.load() == [item])
}

@Test func deleteRemovesAndIsIdempotent() async throws {
    let store = SQLiteConnectionStore(location: .memory)
    let (keep, drop) = (connection("keep"), connection("drop"))
    try await store.save(keep)
    try await store.save(drop)
    try await store.delete(id: drop.id)
    try await store.delete(id: drop.id)
    try await store.delete(id: UUID())
    #expect(try await store.load() == [keep])
}

@Test func storesHostileStringsVerbatim() async throws {
    let store = SQLiteConnectionStore(location: .memory)
    let nasty = connection("'; DROP TABLE connections;--", servers: "x' OR '1'='1", username: "Robert'); DROP TABLE connections;--")
    let unicode = connection("日本語 🚀 \"quoted\" \\ back", username: "ünï")
    try await store.save(nasty)
    try await store.save(unicode)
    #expect(try await store.load() == [nasty, unicode])
    try await store.delete(id: nasty.id)
    #expect(try await store.load() == [unicode])
}

// MARK: Persistence, laziness, schema

@Test func dataSurvivesANewStoreInstanceOnTheSameFile() async throws {
    let file = try temporaryDirectory().appendingPathComponent("db.sqlite")
    let item = connection("persisted", environment: .production, security: .ssl)
    try await SQLiteConnectionStore(location: .file(file)).save(item)
    let reopened = SQLiteConnectionStore(location: .file(file))
    #expect(try await reopened.load() == [item])
    let database = try SQLiteDatabase(path: file.path)
    #expect(try database.userVersion == 1)
}

@Test func initDoesNotTouchTheFilesystemUntilFirstCall() async throws {
    let file = try temporaryDirectory().appendingPathComponent("lazy.sqlite")
    let store = SQLiteConnectionStore(location: .file(file))
    #expect(!FileManager.default.fileExists(atPath: file.path))
    _ = try await store.load()
    #expect(FileManager.default.fileExists(atPath: file.path))
}

@Test func schemaMatchesSpecAndHasNoPasswordColumn() async throws {
    let file = try temporaryDirectory().appendingPathComponent("db.sqlite")
    _ = try await SQLiteConnectionStore(location: .file(file)).load()
    let database = try SQLiteDatabase(path: file.path)
    let columns = try database.query("PRAGMA table_info(connections)") { $0.text(1) }
    #expect(columns == ["id", "name", "bootstrap_servers", "environment", "security", "sasl_mechanism", "username", "sort_order", "created_at", "updated_at"])
    #expect(!columns.contains { $0.lowercased().contains("password") || $0.lowercased().contains("secret") })
    #expect(try database.query("PRAGMA journal_mode") { $0.text(0) } == ["wal"])
}

@Test func migrationFromVersionZeroCreatesSchemaAndSetsVersionOne() throws {
    let database = try SQLiteDatabase(path: ":memory:")
    #expect(try database.userVersion == 0)
    try SQLiteMigrations.migrate(database)
    #expect(try database.userVersion == SQLiteMigrations.latestVersion)
    #expect(SQLiteMigrations.latestVersion == 1)
    #expect(try database.query("SELECT name FROM sqlite_master WHERE name = 'connections'") { $0.text(0) } == ["connections"])
}

@Test func migrationLeavesAnUpToDateDatabaseAlone() throws {
    let database = try SQLiteDatabase(path: ":memory:")
    try SQLiteMigrations.migrate(database)
    try database.run("""
        INSERT INTO connections (id, name, bootstrap_servers, created_at, updated_at) VALUES ('x', 'kept', 'h:1', 1, 1)
        """)
    try SQLiteMigrations.migrate(database)
    #expect(try database.query("SELECT name FROM connections") { $0.text(0) } == ["kept"])
    #expect(try database.userVersion == 1)
}

@Test func newerSchemaVersionIsRejectedWithoutTouchingTheFile() async throws {
    let file = try temporaryDirectory().appendingPathComponent("db.sqlite")
    do {
        let database = try SQLiteDatabase(path: file.path)
        try database.setUserVersion(99)
    }
    let store = SQLiteConnectionStore(location: .file(file))
    await #expect(throws: ConnectionStoreError.unsupportedSchemaVersion(99)) { try await store.load() }
    #expect(await store.takeRecoveryBackup() == nil)
    #expect(try FileManager.default.contentsOfDirectory(atPath: file.deletingLastPathComponent().path).filter { $0.contains("corrupt") }.isEmpty)
}

@Test func rowsWithUnknownValuesAreSkippedNotFatal() async throws {
    let file = try temporaryDirectory().appendingPathComponent("db.sqlite")
    let store = SQLiteConnectionStore(location: .file(file))
    let good = connection("good")
    try await store.save(good)
    let database = try SQLiteDatabase(path: file.path)
    try database.run("""
        INSERT INTO connections (id, name, bootstrap_servers, environment, created_at, updated_at)
        VALUES (?1, 'bad-env', 'h:1', 'moon', 1, 1)
        """, [.text(UUID().uuidString)])
    try database.run("INSERT INTO connections (id, name, bootstrap_servers, created_at, updated_at) VALUES ('not-a-uuid', 'bad-id', 'h:1', 1, 1)")
    #expect(try await store.load() == [good])
}

// MARK: Corruption

@Test func corruptFileIsMovedAsideAndAFreshDatabaseWorks() async throws {
    let directory = try temporaryDirectory()
    let file = directory.appendingPathComponent("kafkamanager.sqlite")
    let garbage = Data((0..<2048).map { UInt8(truncatingIfNeeded: $0 &* 31 &+ 7) })
    try garbage.write(to: file)
    let store = SQLiteConnectionStore(location: .file(file), now: { Date(timeIntervalSince1970: 0) })

    #expect(try await store.load().isEmpty)
    let backup = try #require(await store.takeRecoveryBackup())
    #expect(backup.lastPathComponent == "kafkamanager.sqlite.corrupt-19700101-000000")
    #expect(try Data(contentsOf: backup) == garbage)
    #expect(await store.takeRecoveryBackup() == nil)

    let item = connection("after-recovery")
    try await store.save(item)
    #expect(try await SQLiteConnectionStore(location: .file(file)).load() == [item])
}

@Test func saveAsFirstCallAlsoRecoversAndSaves() async throws {
    let file = try temporaryDirectory().appendingPathComponent("db.sqlite")
    try Data("this is not a database, just text".utf8).write(to: file)
    let store = SQLiteConnectionStore(location: .file(file))
    let item = connection("first-write")
    try await store.save(item)
    #expect(await store.takeRecoveryBackup() != nil)
    #expect(try await store.load() == [item])
}

@Test func repeatedCorruptionDoesNotOverwriteEarlierBackups() async throws {
    let directory = try temporaryDirectory()
    let file = directory.appendingPathComponent("db.sqlite")
    for round in 0..<2 {
        try Data("garbage \(round)".utf8).write(to: file)
        let store = SQLiteConnectionStore(location: .file(file), now: { Date(timeIntervalSince1970: 0) })
        _ = try await store.load()
    }
    let backups = try FileManager.default.contentsOfDirectory(atPath: directory.path).filter { $0.contains("corrupt") && !$0.hasSuffix("-wal") && !$0.hasSuffix("-shm") }
    #expect(backups.sorted() == ["db.sqlite.corrupt-19700101-000000", "db.sqlite.corrupt-19700101-000000-2"])
}

@Test func unopenableLocationThrowsDatabaseErrorWithoutRecovery() async throws {
    let missing = try temporaryDirectory().appendingPathComponent("no-such-dir/db.sqlite")
    let store = SQLiteConnectionStore(location: .file(missing))
    await #expect(throws: ConnectionStoreError.self) { try await store.load() }
    #expect(await store.takeRecoveryBackup() == nil)
}

// MARK: Default location

@Test func defaultURLCreatesDirectoryUnderTheGivenBase() throws {
    let base = try temporaryDirectory()
    let url = try SQLiteConnectionStore.defaultURL(applicationSupport: base)
    #expect(url == base.appendingPathComponent("KafkaManager/kafkamanager.sqlite"))
    var isDirectory: ObjCBool = false
    #expect(FileManager.default.fileExists(atPath: base.appendingPathComponent("KafkaManager").path, isDirectory: &isDirectory) && isDirectory.boolValue)
    #expect(!FileManager.default.fileExists(atPath: url.path))
}

// MARK: Concurrency

@Test func concurrentSavesAndLoadsAreSafe() async throws {
    let file = try temporaryDirectory().appendingPathComponent("db.sqlite")
    let store = SQLiteConnectionStore(location: .file(file))
    let items = (0..<50).map { connection("c\($0)") }
    try await withThrowingTaskGroup(of: Int.self) { group in
        for item in items {
            group.addTask { try await store.save(item); return 0 }
            group.addTask { try await store.load().count }
        }
        for try await _ in group {}
    }
    let loaded = try await store.load()
    #expect(loaded.count == 50)
    #expect(Set(loaded.map(\.id)) == Set(items.map(\.id)))
}

// MARK: In-memory store

@Test func inMemoryStoreSavesUpdatesAndDeletes() async throws {
    let store = InMemoryConnectionStore()
    var a = connection("a")
    let b = connection("b")
    try await store.save(a)
    try await store.save(b)
    a.name = "a2"
    try await store.save(a)
    #expect(try await store.load().map(\.name) == ["a2", "b"])
    try await store.delete(id: a.id)
    try await store.delete(id: a.id)
    #expect(try await store.load() == [b])
    #expect(await store.takeRecoveryBackup() == nil)
}

@Test func storeErrorsHaveUserMessages() {
    let errors: [ConnectionStoreError] = [.database(code: 1, message: "x"), .unsupportedSchemaVersion(2), .recoveryFailed(message: "y")]
    #expect(errors.allSatisfy { !$0.userMessage.isEmpty })
}
