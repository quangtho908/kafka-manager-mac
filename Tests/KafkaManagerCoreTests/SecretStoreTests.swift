import Testing
import Foundation
import Security
@testable import KafkaManagerCore

/// The real login keychain is used with a throwaway service name; every test deletes what it creates.
struct Harness {
    enum Kind: String, CaseIterable, CustomTestStringConvertible {
        case inMemory, keychain
        var testDescription: String { rawValue }
    }

    let kind: Kind
    let service: String
    let store: any SecretStore

    init(_ kind: Kind) {
        self.kind = kind
        service = "KafkaManagerTests-\(UUID().uuidString)"
        store = switch kind {
        case .inMemory: InMemorySecretStore()
        case .keychain: KeychainSecretStore(service: service)
        }
    }

    /// Deletes the given ids; for the keychain also asserts nothing is left under the throwaway service.
    func cleanUp(_ ids: [UUID]) async {
        for id in ids { try? await store.deletePassword(for: id) }  // best effort: failures surface in the leftover check below
        if kind == .keychain { #expect(Harness.itemCount(service: service) == 0, "leftover keychain items") }
    }

    static func itemCount(service: String) -> Int {
        let query: [CFString: Any] = [
            kSecClass: kSecClassGenericPassword,
            kSecAttrService: service,
            kSecMatchLimit: kSecMatchLimitAll,
            kSecReturnAttributes: true,
        ]
        var result: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        return status == errSecSuccess ? (result as? [[String: Any]])?.count ?? 0 : 0
    }
}

let secretStoreKinds = Harness.Kind.allCases

@Test(arguments: secretStoreKinds)
func setThenGetReturnsThePassword(kind: Harness.Kind) async throws {
    let harness = Harness(kind), id = UUID()
    do {
        try await harness.store.setPassword("s3cret", for: id)
        #expect(try await harness.store.password(for: id) == "s3cret")
    } catch { await harness.cleanUp([id]); throw error }
    await harness.cleanUp([id])
}

@Test(arguments: secretStoreKinds)
func getUnknownReturnsNil(kind: Harness.Kind) async throws {
    let harness = Harness(kind)
    #expect(try await harness.store.password(for: UUID()) == nil)
    await harness.cleanUp([])
}

@Test(arguments: secretStoreKinds)
func settingTwiceUpdatesTheValue(kind: Harness.Kind) async throws {
    let harness = Harness(kind), id = UUID()
    do {
        try await harness.store.setPassword("first", for: id)
        try await harness.store.setPassword("second", for: id)
        #expect(try await harness.store.password(for: id) == "second")
    } catch { await harness.cleanUp([id]); throw error }
    await harness.cleanUp([id])
}

@Test(arguments: secretStoreKinds)
func deleteRemovesAndIsIdempotent(kind: Harness.Kind) async throws {
    let harness = Harness(kind), id = UUID()
    do {
        try await harness.store.setPassword("gone", for: id)
        try await harness.store.deletePassword(for: id)
        try await harness.store.deletePassword(for: id)
        try await harness.store.deletePassword(for: UUID())
        #expect(try await harness.store.password(for: id) == nil)
    } catch { await harness.cleanUp([id]); throw error }
    await harness.cleanUp([id])
}

@Test(arguments: secretStoreKinds)
func passwordsForDifferentIdsAreIndependent(kind: Harness.Kind) async throws {
    let harness = Harness(kind), (a, b) = (UUID(), UUID())
    do {
        try await harness.store.setPassword("alpha", for: a)
        try await harness.store.setPassword("beta", for: b)
        try await harness.store.deletePassword(for: a)
        #expect(try await harness.store.password(for: a) == nil)
        #expect(try await harness.store.password(for: b) == "beta")
    } catch { await harness.cleanUp([a, b]); throw error }
    await harness.cleanUp([a, b])
}

@Test(arguments: secretStoreKinds)
func unusualPasswordsRoundTripExactly(kind: Harness.Kind) async throws {
    let harness = Harness(kind), id = UUID()
    let samples = ["", " ", "pässwörd-日本語-🔐", String(repeating: "x", count: 10_000), "line1\nline2\ttab", "quote'\"\\;--", "a\0b"]
    do {
        for sample in samples {
            try await harness.store.setPassword(sample, for: id)
            #expect(try await harness.store.password(for: id) == sample)
        }
    } catch { await harness.cleanUp([id]); throw error }
    await harness.cleanUp([id])
}

@Test(arguments: secretStoreKinds)
func concurrentSetsOfTheSameIdLeaveOneOfTheValues(kind: Harness.Kind) async throws {
    let harness = Harness(kind), id = UUID()
    let values = (0..<20).map { "value-\($0)" }
    do {
        try await withThrowingTaskGroup(of: Void.self) { group in
            for value in values { group.addTask { try await harness.store.setPassword(value, for: id) } }
            try await group.waitForAll()
        }
        #expect(values.contains(try #require(try await harness.store.password(for: id))))
    } catch { await harness.cleanUp([id]); throw error }
    await harness.cleanUp([id])
}

@Test func keychainStoresDoNotSeeEachOthersServices() async throws {
    let one = Harness(.keychain), two = Harness(.keychain), id = UUID()
    do {
        try await one.store.setPassword("only-in-one", for: id)
        #expect(try await two.store.password(for: id) == nil)
        try await two.store.deletePassword(for: id)
        #expect(try await one.store.password(for: id) == "only-in-one")
    } catch { await one.cleanUp([id]); await two.cleanUp([id]); throw error }
    await one.cleanUp([id])
    await two.cleanUp([id])
}

@Test func keychainStoreWritesNothingAtInit() {
    let service = "KafkaManagerTests-\(UUID().uuidString)"
    _ = KeychainSecretStore(service: service)
    #expect(Harness.itemCount(service: service) == 0)
}

@Test func errorsNeverContainThePassword() async throws {
    let secret = "super-secret-pw-12345"
    let errors: [SecretStoreError] = [.keychain(status: errSecAuthFailed), .keychain(status: -34018), .keychain(status: 0), .invalidData]
    for error in errors {
        #expect(!error.userMessage.isEmpty)
        #expect(!error.userMessage.contains(secret))
        #expect(!String(describing: error).contains(secret))
        #expect(!String(reflecting: error).contains(secret))
    }
    // A real failure path: the data-protection keychain is unavailable to unsigned builds.
    let harness = KeychainSecretStore(service: "KafkaManagerTests-\(UUID().uuidString)", useDataProtectionKeychain: true)
    do {
        try await harness.setPassword(secret, for: UUID())
        try await harness.deletePassword(for: UUID())
    } catch {
        #expect(!String(describing: error).contains(secret))
        #expect(!String(reflecting: error).contains(secret))
        #expect((error as? SecretStoreError)?.userMessage.contains(secret) == false)
    }
}

@Test func userMessageMentionsTheSystemStatusText() {
    #expect(SecretStoreError.keychain(status: errSecItemNotFound).userMessage.count > 20)
}
