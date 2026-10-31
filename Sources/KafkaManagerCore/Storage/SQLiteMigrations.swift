import Foundation

/// Schema versions tracked in `PRAGMA user_version`. To add version N+1: bump `latestVersion` and add a case in `apply`.
enum SQLiteMigrations {
    static let latestVersion = 1

    static func migrate(_ database: SQLiteDatabase) throws {
        var version = try database.userVersion
        guard version <= latestVersion else { throw ConnectionStoreError.unsupportedSchemaVersion(version) }
        while version < latestVersion {
            version += 1
            let target = version
            try database.transaction {
                try apply(target, to: database)
                try database.setUserVersion(target)
            }
        }
    }

    private static func apply(_ version: Int, to database: SQLiteDatabase) throws {
        switch version {
        case 1:
            try database.execute("""
                CREATE TABLE connections (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  bootstrap_servers TEXT NOT NULL,
                  environment TEXT NOT NULL DEFAULT 'local',
                  security TEXT NOT NULL DEFAULT 'plaintext',
                  sasl_mechanism TEXT,
                  username TEXT,
                  sort_order INTEGER NOT NULL DEFAULT 0,
                  created_at REAL NOT NULL,
                  updated_at REAL NOT NULL
                )
                """)
        default:
            throw ConnectionStoreError.unsupportedSchemaVersion(version)
        }
    }
}
