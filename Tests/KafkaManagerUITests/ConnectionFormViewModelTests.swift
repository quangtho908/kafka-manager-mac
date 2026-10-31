import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private let secret = "s3cr3t-pw"

private func sasl(name: String = "prod") -> ClusterConnection {
    ClusterConnection(name: name, bootstrapServers: "b1:9092", environment: .production, security: .saslSsl,
                      saslMechanism: .scramSha512, username: "alice")
}

@MainActor
private func fill(_ form: ConnectionFormViewModel, name: String = "dev", servers: String = " b1 , b2:9093 ") {
    form.name = name
    form.bootstrapServers = servers
}

@MainActor
private func fillSASL(_ form: ConnectionFormViewModel, password: String = secret) {
    fill(form)
    form.security = .saslSsl
    form.saslMechanism = .plain
    form.username = " alice "
    form.password = password
}

@MainActor
private func savedConnection(_ form: ConnectionFormViewModel) -> ClusterConnection? {
    if case .saved(let connection) = form.saveState { connection } else { nil }
}

@Suite("ConnectionFormViewModel") @MainActor
struct ConnectionFormViewModelTests {
    @Test func initDoesNoIO() async {
        let store = SpyConnectionStore()
        let secrets = SpySecretStore()
        _ = ConnectionFormViewModel(mode: .edit(sasl()), store: store, secrets: secrets, existingNames: [])
        #expect(await store.callCount == 0)
        #expect(await secrets.callCount == 0)
    }

    @Test func savesNormalizedBootstrapAndTrimmedFields() async throws {
        let store = InMemoryConnectionStore()
        let form = ConnectionFormViewModel(mode: .new, store: store, secrets: InMemorySecretStore(), existingNames: [])
        fillSASL(form)
        form.name = "  dev  "
        await form.save()
        let saved = try #require(savedConnection(form))
        #expect(saved.name == "dev")
        #expect(saved.bootstrapServers == "b1:9092,b2:9093")
        #expect(saved.username == "alice")
        #expect(try await store.load() == [saved])
    }

    @Test func secretIsWrittenBeforeTheRow() async {
        let log = EventLog()
        let form = ConnectionFormViewModel(mode: .new, store: SpyConnectionStore(log: log), secrets: SpySecretStore(log: log), existingNames: [])
        fillSASL(form)
        await form.save()
        #expect(await log.events == ["secret.set", "store.save"])
    }

    @Test func failedSecretWriteSkipsTheRow() async {
        let log = EventLog()
        let secrets = SpySecretStore(log: log)
        await secrets.setFailures(set: true)
        let form = ConnectionFormViewModel(mode: .new, store: SpyConnectionStore(log: log), secrets: secrets, existingNames: [])
        fillSASL(form)
        await form.save()
        #expect(await log.events == ["secret.set"])
        guard case .failed = form.saveState else { Issue.record("expected failure"); return }
    }

    @Test func storeFailureRollsBackNewProfileSecret() async throws {
        let log = EventLog()
        let store = SpyConnectionStore(log: log)
        await store.setFailures(save: true)
        let secrets = SpySecretStore(log: log)
        let form = ConnectionFormViewModel(mode: .new, store: store, secrets: secrets, existingNames: [])
        fillSASL(form)
        await form.save()
        #expect(await log.events == ["secret.set", "store.save", "secret.delete"])
        #expect(try await secrets.password(for: form.connectionID) == nil)
        guard case .failed = form.saveState else { Issue.record("expected failure"); return }
    }

    @Test func storeFailureNeverDeletesAnOldSecretOnEdit() async throws {
        let existing = sasl()
        let log = EventLog()
        let store = SpyConnectionStore(connections: [existing], log: log)
        await store.setFailures(save: true)
        let secrets = SpySecretStore(log: log)
        try await secrets.setPassword("old-pw", for: existing.id)
        let form = ConnectionFormViewModel(mode: .edit(existing), store: store, secrets: secrets, existingNames: [])
        form.password = "new-pw"
        await form.save()
        // First "secret.set" is the test's own setup; the save then writes and fails without any delete.
        #expect(await log.events == ["secret.set", "secret.set", "store.save"])
        #expect(try await secrets.password(for: existing.id) != nil)
    }

    @Test func blankPasswordKeepsTheExistingSecretOnEdit() async throws {
        let existing = sasl()
        let secrets = InMemorySecretStore()
        try await secrets.setPassword("old-pw", for: existing.id)
        let form = ConnectionFormViewModel(mode: .edit(existing), store: InMemoryConnectionStore(connections: [existing]), secrets: secrets, existingNames: [])
        form.name = "renamed"
        await form.save()
        #expect(savedConnection(form)?.name == "renamed")
        #expect(try await secrets.password(for: existing.id) == "old-pw")
    }

    @Test func nonBlankPasswordReplacesTheSecret() async throws {
        let existing = sasl()
        let secrets = InMemorySecretStore()
        try await secrets.setPassword("old-pw", for: existing.id)
        let form = ConnectionFormViewModel(mode: .edit(existing), store: InMemoryConnectionStore(connections: [existing]), secrets: secrets, existingNames: [])
        form.password = "new-pw"
        await form.save()
        #expect(try await secrets.password(for: existing.id) == "new-pw")
        #expect(form.hasStoredPassword)
    }

    @Test func clearStoredPasswordDeletesIt() async throws {
        let existing = sasl()
        let secrets = InMemorySecretStore()
        try await secrets.setPassword("old-pw", for: existing.id)
        let form = ConnectionFormViewModel(mode: .edit(existing), store: InMemoryConnectionStore(connections: [existing]), secrets: secrets, existingNames: [])
        await form.loadStoredPasswordFlag()
        #expect(form.hasStoredPassword)
        form.clearStoredPassword = true
        await form.save()
        #expect(try await secrets.password(for: existing.id) == nil)
        #expect(!form.hasStoredPassword)
        #expect(!form.clearStoredPassword)
    }

    @Test func blankPasswordOnNewProfileStoresNoSecret() async throws {
        let log = EventLog()
        let form = ConnectionFormViewModel(mode: .new, store: SpyConnectionStore(log: log), secrets: SpySecretStore(log: log), existingNames: [])
        fillSASL(form, password: "")
        await form.save()
        #expect(await log.events == ["store.save"])
        #expect(savedConnection(form) != nil)
    }

    @Test func switchingToPlaintextDropsSASLFieldsAndDeletesTheSecret() async throws {
        let existing = sasl()
        let secrets = InMemorySecretStore()
        try await secrets.setPassword("old-pw", for: existing.id)
        let form = ConnectionFormViewModel(mode: .edit(existing), store: InMemoryConnectionStore(connections: [existing]), secrets: secrets, existingNames: [])
        form.security = .plaintext
        form.password = "ignored"
        await form.save()
        let saved = try #require(savedConnection(form))
        #expect(saved.saslMechanism == nil)
        #expect(saved.username == nil)
        #expect(try await secrets.password(for: existing.id) == nil)
    }

    @Test func plaintextNewProfileTouchesNoSecret() async {
        let secrets = SpySecretStore()
        let form = ConnectionFormViewModel(mode: .new, store: InMemoryConnectionStore(), secrets: secrets, existingNames: [])
        fill(form)
        await form.save()
        #expect(await secrets.callCount == 0)
    }

    @Test func duplicateGetsFreshIDCopyNameAndNoSecret() async throws {
        let original = sasl()
        let secrets = SpySecretStore()
        try await secrets.setPassword("old-pw", for: original.id)
        let store = InMemoryConnectionStore(connections: [original])
        let form = ConnectionFormViewModel(mode: .duplicate(original), store: store, secrets: secrets, existingNames: ["prod"])
        #expect(form.connectionID != original.id)
        #expect(form.name == "prod copy")
        #expect(form.password.isEmpty)
        await form.loadStoredPasswordFlag()
        #expect(!form.hasStoredPassword)
        await form.save()
        let saved = try #require(savedConnection(form))
        #expect(saved.id != original.id)
        #expect(try await secrets.password(for: saved.id) == nil)
        #expect(try await secrets.password(for: original.id) == "old-pw")
        #expect(try await store.load().count == 2)
    }

    @Test func storedPasswordFlagOnlyLoadsWhenEditing() async throws {
        let secrets = SpySecretStore()
        let new = ConnectionFormViewModel(mode: .new, store: InMemoryConnectionStore(), secrets: secrets, existingNames: [])
        await new.loadStoredPasswordFlag()
        #expect(await secrets.callCount == 0)

        let existing = sasl()
        try await secrets.setPassword("pw", for: existing.id)
        let edit = ConnectionFormViewModel(mode: .edit(existing), store: InMemoryConnectionStore(), secrets: secrets, existingNames: [])
        #expect(!edit.hasStoredPassword)
        await edit.loadStoredPasswordFlag()
        #expect(edit.hasStoredPassword)
    }

    @Test func canSaveFollowsErrorsNotWarnings() {
        let form = ConnectionFormViewModel(mode: .new, store: InMemoryConnectionStore(), secrets: InMemorySecretStore(), existingNames: ["Dev"])
        #expect(!form.canSave)
        fill(form, servers: "a,a")
        #expect(form.issues.allSatisfy { $0.severity == .warning })
        #expect(form.issues.count == 2)
        #expect(form.canSave)
        form.bootstrapServers = "bad host"
        #expect(!form.canSave)
    }

    @Test func nameUniquenessExcludesTheEditedProfile() {
        let existing = sasl(name: "prod")
        let editing = ConnectionFormViewModel(mode: .edit(existing), store: InMemoryConnectionStore(), secrets: InMemorySecretStore(), existingNames: [])
        #expect(editing.issues.isEmpty)
        let clashing = ConnectionFormViewModel(mode: .edit(existing), store: InMemoryConnectionStore(), secrets: InMemorySecretStore(), existingNames: ["PROD"])
        #expect(clashing.issues.map(\.field) == [.name])
    }

    @Test func saveWithErrorsDoesNothing() async {
        let store = SpyConnectionStore()
        let form = ConnectionFormViewModel(mode: .new, store: store, secrets: InMemorySecretStore(), existingNames: [])
        await form.save()
        #expect(form.saveState == .idle)
        #expect(await store.callCount == 0)
    }

    @Test func passwordIsClearedAfterSaveAndNeverAppearsInState() async throws {
        let store = SpyConnectionStore()
        let form = ConnectionFormViewModel(mode: .new, store: store, secrets: InMemorySecretStore(), existingNames: [])
        fillSASL(form)
        await form.save()
        #expect(form.password.isEmpty)
        #expect(!String(describing: form.saveState).contains(secret))
        #expect(!String(reflecting: form.saveState).contains(secret))
        #expect(try await store.load().allSatisfy { !String(describing: $0).contains(secret) })
    }

    @Test func failureMessagesNeverContainPasswordOrRawSQL() async {
        let store = SpyConnectionStore()
        await store.setFailures(save: true)
        let form = ConnectionFormViewModel(mode: .new, store: store, secrets: InMemorySecretStore(), existingNames: [])
        fillSASL(form)
        await form.save()
        guard case .failed(let message) = form.saveState else { Issue.record("expected failure"); return }
        #expect(!message.contains(secret))
        #expect(!message.contains("INSERT"))
        #expect(!String(describing: form.saveState).contains(secret))
        #expect(form.password == secret)  // kept so the user can retry
    }

    @Test func secretFailureMessageIsUserFacing() async {
        let secrets = SpySecretStore()
        await secrets.setFailures(set: true)
        let form = ConnectionFormViewModel(mode: .new, store: InMemoryConnectionStore(), secrets: secrets, existingNames: [])
        fillSASL(form)
        await form.save()
        guard case .failed(let message) = form.saveState else { Issue.record("expected failure"); return }
        #expect(message == SecretStoreError.keychain(status: -25293).userMessage)
        #expect(!message.contains(secret))
    }
}
