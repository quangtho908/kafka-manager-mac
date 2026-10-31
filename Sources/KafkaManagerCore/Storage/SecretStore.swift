import Foundation
import Security

/// Storage for profile passwords. Implementations must never persist them anywhere but the Keychain
/// (or memory, for tests) and must never include a password in an error or description.
public protocol SecretStore: Sendable {
    /// nil when nothing is stored for `id`.
    func password(for id: UUID) async throws -> String?
    /// Adds or replaces the password for `id`.
    func setPassword(_ password: String, for id: UUID) async throws
    /// Removing a password that isn't stored is not an error.
    func deletePassword(for id: UUID) async throws
}

public enum SecretStoreError: Error, Sendable, Equatable {
    /// An `OSStatus` from the Security framework.
    case keychain(status: Int32)
    /// A stored item that isn't valid UTF-8 text.
    case invalidData

    public var userMessage: String {
        switch self {
        case .keychain(let status):
            let detail = SecCopyErrorMessageString(status, nil) as String? ?? String(status)
            return String(localized: "The Keychain couldn't be used (\(detail)).")
        case .invalidData:
            return String(localized: "The saved password couldn't be read.")
        }
    }
}
