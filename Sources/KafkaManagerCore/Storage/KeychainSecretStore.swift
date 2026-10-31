import Foundation
import Security

/// `SecretStore` over generic-password Keychain items: service = `service`, account = profile id.
///
/// A struct, not an actor: it has no mutable state. SecItem calls can block (locked keychain, access prompt, securityd),
/// so they run on a private serial queue instead of occupying Swift concurrency's cooperative threads. Every call is on
/// demand; `init` does no I/O.
public struct KeychainSecretStore: SecretStore {
    public static let defaultService = "KafkaManager"

    private let service: String
    private let useDataProtectionKeychain: Bool
    private let queue = DispatchQueue(label: "KafkaManager.KeychainSecretStore", qos: .userInitiated)

    /// - Parameter useDataProtectionKeychain: the data-protection keychain needs a signed app with entitlements
    ///   (unsigned builds get -34018); the default, the legacy login keychain, works for unsigned development builds.
    public init(service: String = KeychainSecretStore.defaultService, useDataProtectionKeychain: Bool = false) {
        self.service = service
        self.useDataProtectionKeychain = useDataProtectionKeychain
    }

    public func password(for id: UUID) async throws -> String? {
        try await onQueue { try readPassword(for: id) }
    }

    public func setPassword(_ password: String, for id: UUID) async throws {
        try await onQueue { try writePassword(password, for: id) }
    }

    public func deletePassword(for id: UUID) async throws {
        try await onQueue { try removePassword(for: id) }
    }

    // MARK: Keychain operations (blocking; always called on `queue`)

    private func readPassword(for id: UUID) throws -> String? {
        var query = identity(of: id)
        query[kSecReturnData] = true
        query[kSecMatchLimit] = kSecMatchLimitOne
        var result: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        if status == errSecItemNotFound { return nil }
        try check(status)
        guard let data = result as? Data, let password = String(data: data, encoding: .utf8) else {
            throw SecretStoreError.invalidData
        }
        return password
    }

    private func writePassword(_ password: String, for id: UUID) throws {
        let data = Data(password.utf8)
        var add = identity(of: id)
        add[kSecValueData] = data
        let status = SecItemAdd(add as CFDictionary, nil)
        guard status == errSecDuplicateItem else { return try check(status) }
        if data.isEmpty {
            // SecItemUpdate reports success but keeps the old value when the new data is empty.
            try removePassword(for: id)
            try check(SecItemAdd(add as CFDictionary, nil))
        } else {
            try check(SecItemUpdate(identity(of: id) as CFDictionary, [kSecValueData: data] as CFDictionary))
        }
    }

    private func removePassword(for id: UUID) throws {
        let status = SecItemDelete(identity(of: id) as CFDictionary)
        if status != errSecItemNotFound { try check(status) }
    }

    private func identity(of id: UUID) -> [CFString: Any] {
        var query: [CFString: Any] = [
            kSecClass: kSecClassGenericPassword,
            kSecAttrService: service,
            kSecAttrAccount: id.uuidString,
        ]
        if useDataProtectionKeychain { query[kSecUseDataProtectionKeychain] = true }
        return query
    }

    private func check(_ status: OSStatus) throws {
        if status != errSecSuccess { throw SecretStoreError.keychain(status: status) }
    }

    private func onQueue<Value: Sendable>(_ work: @escaping @Sendable () throws -> Value) async throws -> Value {
        try await withCheckedThrowingContinuation { continuation in
            queue.async { continuation.resume(with: Result { try work() }) }
        }
    }
}
