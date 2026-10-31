import Foundation

/// Non-persistent store for tests and previews.
public actor InMemorySecretStore: SecretStore {
    private var passwords: [UUID: String] = [:]

    public init() {}

    public func password(for id: UUID) async throws -> String? {
        passwords[id]
    }

    public func setPassword(_ password: String, for id: UUID) async throws {
        passwords[id] = password
    }

    public func deletePassword(for id: UUID) async throws {
        passwords[id] = nil
    }
}
