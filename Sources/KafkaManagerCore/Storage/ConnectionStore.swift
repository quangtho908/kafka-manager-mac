import Foundation

/// Persistence for saved cluster profiles. Never holds secrets; passwords live in the Keychain.
public protocol ConnectionStore: Sendable {
    /// Profiles ordered by their sort order, then creation time.
    func load() async throws -> [ClusterConnection]
    /// Inserts or updates by `id`. An update keeps the profile's position and creation time.
    func save(_ connection: ClusterConnection) async throws
    /// Removing an unknown id is not an error.
    func delete(id: UUID) async throws
    /// If the store had to replace an unreadable database, returns the backup location once, then nil.
    func takeRecoveryBackup() async -> URL?
}

public extension ConnectionStore {
    func takeRecoveryBackup() async -> URL? { nil }
}

public enum ConnectionStoreError: Error, Sendable, Equatable {
    case database(code: Int32, message: String)
    /// The database was written by a newer version of the app.
    case unsupportedSchemaVersion(Int)
    case recoveryFailed(message: String)

    public var userMessage: String {
        switch self {
        case .database:
            String(localized: "The saved connections couldn't be read or written.")
        case .unsupportedSchemaVersion:
            String(localized: "The saved connections were created by a newer version of the app.")
        case .recoveryFailed:
            String(localized: "The saved connections file is damaged and couldn't be replaced.")
        }
    }
}
