import Foundation
import SQLite3

/// `ConnectionStore` over the system SQLite. The database is opened lazily on the first call.
public actor SQLiteConnectionStore: ConnectionStore {
    public enum Location: Sendable, Equatable {
        case file(URL)
        case memory
    }

    private static let directoryName = "KafkaManager"
    private static let fileName = "kafkamanager.sqlite"
    private static let sidecarSuffixes = ["-wal", "-shm"]

    private static let selectSQL = """
        SELECT id, name, bootstrap_servers, environment, security, sasl_mechanism, username
        FROM connections ORDER BY sort_order, created_at, rowid
        """

    // New rows go to the end; updates leave sort_order and created_at alone.
    private static let upsertSQL = """
        INSERT INTO connections
          (id, name, bootstrap_servers, environment, security, sasl_mechanism, username, sort_order, created_at, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, (SELECT COALESCE(MAX(sort_order) + 1, 0) FROM connections), ?8, ?8)
        ON CONFLICT(id) DO UPDATE SET
          name = excluded.name,
          bootstrap_servers = excluded.bootstrap_servers,
          environment = excluded.environment,
          security = excluded.security,
          sasl_mechanism = excluded.sasl_mechanism,
          username = excluded.username,
          updated_at = excluded.updated_at
        """

    private let location: Location
    private let now: @Sendable () -> Date
    private var database: SQLiteDatabase?
    private var recoveryBackup: URL?

    /// Does not touch the filesystem; the file is opened (and created) by the first call.
    public init(location: Location, now: @escaping @Sendable () -> Date = { Date() }) {
        self.location = location
        self.now = now
    }

    /// `~/Library/Application Support/KafkaManager/kafkamanager.sqlite`, creating the directory.
    public static func defaultURL() throws -> URL {
        let base = try FileManager.default.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
        return try defaultURL(applicationSupport: base)
    }

    static func defaultURL(applicationSupport base: URL) throws -> URL {
        let directory = base.appendingPathComponent(directoryName, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory.appendingPathComponent(fileName)
    }

    public func load() async throws -> [ClusterConnection] {
        try perform { database in
            try database.query(Self.selectSQL) { row in Self.connection(from: row) }
        }
    }

    public func save(_ connection: ClusterConnection) async throws {
        let timestamp = now().timeIntervalSince1970
        try perform { database in
            try database.run(Self.upsertSQL, [
                .text(connection.id.uuidString),
                .text(connection.name),
                .text(connection.bootstrapServers),
                .text(connection.environment.rawValue),
                .text(connection.security.rawValue),
                connection.saslMechanism.map { .text($0.rawValue) } ?? .null,
                connection.username.map { .text($0) } ?? .null,
                .real(timestamp),
            ])
        }
    }

    public func delete(id: UUID) async throws {
        try perform { database in
            try database.run("DELETE FROM connections WHERE id = ?1", [.text(id.uuidString)])
        }
    }

    public func takeRecoveryBackup() async -> URL? {
        defer { recoveryBackup = nil }
        return recoveryBackup
    }

    // MARK: Opening and recovery

    private func perform<Result>(_ body: (SQLiteDatabase) throws -> Result) throws -> Result {
        do {
            return try body(try openIfNeeded())
        } catch let error as SQLiteError {
            throw ConnectionStoreError.database(code: error.code, message: error.message)
        }
    }

    private func openIfNeeded() throws -> SQLiteDatabase {
        if let database { return database }
        let opened = try open()
        database = opened
        return opened
    }

    private func open() throws -> SQLiteDatabase {
        guard case .file(let url) = location else { return try Self.prepared(path: ":memory:") }
        do {
            return try Self.prepared(path: url.path)
        } catch let error as SQLiteError where Self.isCorruption(error) {
            recoveryBackup = try moveAside(url)
            return try Self.prepared(path: url.path)
        }
    }

    private static func prepared(path: String) throws -> SQLiteDatabase {
        let database = try SQLiteDatabase(path: path)
        try database.execute("PRAGMA journal_mode = WAL")
        try database.execute("PRAGMA foreign_keys = ON")
        try SQLiteMigrations.migrate(database)
        return database
    }

    private static func isCorruption(_ error: SQLiteError) -> Bool {
        error.primaryCode == SQLITE_CORRUPT || error.primaryCode == SQLITE_NOTADB
    }

    /// Renames the unreadable file (and its WAL/SHM leftovers) to `<name>.corrupt-<timestamp>`; nothing is deleted.
    private func moveAside(_ url: URL) throws -> URL {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "UTC")
        formatter.dateFormat = "yyyyMMdd-HHmmss"
        let stem = "\(url.lastPathComponent).corrupt-\(formatter.string(from: now()))"
        let manager = FileManager.default
        var backup = url.deletingLastPathComponent().appendingPathComponent(stem)
        var attempt = 1
        while manager.fileExists(atPath: backup.path) {
            attempt += 1
            backup = url.deletingLastPathComponent().appendingPathComponent("\(stem)-\(attempt)")
        }
        do {
            try manager.moveItem(at: url, to: backup)
            for suffix in Self.sidecarSuffixes {
                let sidecar = URL(fileURLWithPath: url.path + suffix)
                if manager.fileExists(atPath: sidecar.path) {
                    try manager.moveItem(at: sidecar, to: URL(fileURLWithPath: backup.path + suffix))
                }
            }
        } catch {
            throw ConnectionStoreError.recoveryFailed(message: error.localizedDescription)
        }
        return backup
    }

    // Rows with an unknown id or enum value are skipped, not deleted, so one bad row doesn't hide the rest.
    private static func connection(from row: SQLiteRow) -> ClusterConnection? {
        guard let idText = row.text(0), let id = UUID(uuidString: idText),
              let name = row.text(1), let bootstrapServers = row.text(2),
              let environment = row.text(3).flatMap(ClusterConnection.Environment.init(rawValue:)),
              let security = row.text(4).flatMap(ClusterConnection.Security.init(rawValue:))
        else { return nil }
        let mechanismText = row.text(5)
        let mechanism = mechanismText.flatMap(ClusterConnection.SASLMechanism.init(rawValue:))
        if mechanismText != nil, mechanism == nil { return nil }
        return ClusterConnection(
            id: id,
            name: name,
            bootstrapServers: bootstrapServers,
            environment: environment,
            security: security,
            saslMechanism: mechanism,
            username: row.text(6)
        )
    }
}
