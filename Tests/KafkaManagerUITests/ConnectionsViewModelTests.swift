import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private func profile(_ name: String) -> ClusterConnection {
    ClusterConnection(name: name, bootstrapServers: "\(name):9092")
}

@Suite("ConnectionsViewModel") @MainActor
struct ConnectionsViewModelTests {
    @Test func initIsLazy() async {
        let store = SpyConnectionStore(connections: [profile("a")])
        let secrets = SpySecretStore()
        let model = ConnectionsViewModel(store: store, secrets: secrets)
        #expect(model.state == .idle)
        #expect(await store.callCount == 0)
        #expect(await secrets.callCount == 0)
    }

    @Test func loadPopulatesStateAndOnlyRunsOnceUnlessRefreshed() async {
        let store = SpyConnectionStore(connections: [profile("a")])
        let model = ConnectionsViewModel(store: store, secrets: InMemorySecretStore())
        await model.load()
        #expect(model.state.value?.map(\.name) == ["a"])
        let afterFirst = await store.callCount
        await model.load()
        #expect(await store.callCount == afterFirst)
        await model.load(refresh: true)
        #expect(await store.callCount > afterFirst)
    }

    @Test func loadFailureIsReportedAndRetryable() async {
        let store = SpyConnectionStore(connections: [profile("a")])
        await store.setFailures(load: true)
        let model = ConnectionsViewModel(store: store, secrets: InMemorySecretStore())
        await model.load()
        #expect(model.state.failureMessage == ConnectionStoreError.database(code: 1, message: "").userMessage)
        #expect(!(model.state.failureMessage ?? "").contains("SELECT"))
        await store.setFailures()
        await model.load()  // failed state needs an explicit refresh
        #expect(model.state.value == nil)
        await model.load(refresh: true)
        #expect(model.state.value?.count == 1)
    }

    @Test func recoveryNoticeIsShownOnceAndDismissable() async {
        let url = URL(fileURLWithPath: "/tmp/connections.corrupt-1")
        let store = SpyConnectionStore(connections: [profile("a")], backup: url)
        let model = ConnectionsViewModel(store: store, secrets: InMemorySecretStore())
        await model.load()
        #expect(model.recoveredBackup == url)
        model.dismissRecoveryNotice()
        #expect(model.recoveredBackup == nil)
        await model.load(refresh: true)
        #expect(model.recoveredBackup == nil)
    }

    @Test func cancelledLoadPublishesNothing() async {
        let gate = Gate()
        let url = URL(fileURLWithPath: "/tmp/x")
        let store = SpyConnectionStore(connections: [profile("a")], gate: gate, backup: url)
        let model = ConnectionsViewModel(store: store, secrets: InMemorySecretStore())
        let task = Task { await model.load() }
        while await store.callCount == 0 { await Task.yield() }
        task.cancel()
        await gate.open()
        await task.value
        #expect(model.state == .idle)
        #expect(model.recoveredBackup == nil)
    }

    @Test func deleteRemovesSecretBeforeRowAndUpdatesState() async {
        let log = EventLog()
        let a = profile("a"), b = profile("b")
        let model = ConnectionsViewModel(store: SpyConnectionStore(connections: [a, b], log: log), secrets: SpySecretStore(log: log))
        await model.load()
        model.selection = a.id
        await model.delete(id: a.id)
        #expect(await log.events.suffix(2) == ["secret.delete", "store.delete"])
        #expect(model.state.value == [b])
        #expect(model.selection == nil)
        #expect(model.deleteError == nil)
    }

    @Test func failedSecretDeleteKeepsTheRow() async {
        let log = EventLog()
        let a = profile("a")
        let secrets = SpySecretStore(log: log)
        await secrets.setFailures(delete: true)
        let model = ConnectionsViewModel(store: SpyConnectionStore(connections: [a], log: log), secrets: secrets)
        await model.load()
        await model.delete(id: a.id)
        #expect(await !log.events.contains("store.delete"))
        #expect(model.state.value == [a])
        #expect(model.deleteError == SecretStoreError.keychain(status: -25293).userMessage)
        model.dismissDeleteError()
        #expect(model.deleteError == nil)
    }

    @Test func failedRowDeleteKeepsTheRowAndReports() async {
        let a = profile("a")
        let store = SpyConnectionStore(connections: [a])
        await store.setFailures(delete: true)
        let model = ConnectionsViewModel(store: store, secrets: InMemorySecretStore())
        await model.load()
        await model.delete(id: a.id)
        #expect(model.state.value == [a])
        #expect(model.deleteError != nil)
        #expect(!(model.deleteError ?? "").contains("DELETE"))
    }

    @Test func existingNamesExcludeTheGivenProfile() async {
        let a = profile("a"), b = profile("b")
        let model = ConnectionsViewModel(store: InMemoryConnectionStore(connections: [a, b]), secrets: InMemorySecretStore())
        #expect(model.existingNames(excluding: nil).isEmpty)
        await model.load()
        #expect(model.existingNames(excluding: nil) == ["a", "b"])
        #expect(model.existingNames(excluding: a.id) == ["b"])
    }

    @Test func didSaveUpsertsWithoutReloading() async {
        let a = profile("a")
        let store = SpyConnectionStore(connections: [a])
        let model = ConnectionsViewModel(store: store, secrets: InMemorySecretStore())
        model.didSave(profile("early"))
        #expect(model.state == .idle)
        await model.load()
        let calls = await store.callCount
        var renamed = a
        renamed.name = "renamed"
        let fresh = profile("fresh")
        model.didSave(renamed)
        model.didSave(fresh)
        #expect(model.state.value == [renamed, fresh])
        #expect(await store.callCount == calls)
    }

    @Test func duplicateDraftIsADuplicateMode() {
        let a = profile("a")
        let model = ConnectionsViewModel(store: InMemoryConnectionStore(), secrets: InMemorySecretStore())
        #expect(model.duplicateDraft(of: a) == .duplicate(a))
    }
}
