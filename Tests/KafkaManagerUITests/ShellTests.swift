import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private func profile(_ name: String) -> ClusterConnection {
    ClusterConnection(name: name, bootstrapServers: "\(name):9092")
}

@Suite("ShellViewModel") @MainActor
struct ShellViewModelTests {
    private func makeShell(_ connections: [ClusterConnection]) async -> ShellViewModel {
        let model = ConnectionsViewModel(store: InMemoryConnectionStore(connections: connections), secrets: InMemorySecretStore())
        await model.load()
        return ShellViewModel(connections: model)
    }

    @Test func startsWithNothingSelected() async {
        let shell = await makeShell([profile("a")])
        #expect(shell.selectedConnection == nil)
        #expect(shell.selectedSection == nil)
    }

    @Test func selectingAConnectionDoesNotPickASection() async {
        let a = profile("a")
        let shell = await makeShell([a])
        shell.connections.selection = a.id
        #expect(shell.selectedConnection == a)
        #expect(shell.selectedSection == nil)
    }

    @Test func sectionBelongsToItsConnection() async {
        let a = profile("a"), b = profile("b")
        let shell = await makeShell([a, b])
        shell.connections.selection = a.id
        shell.selectedSection = .topics
        #expect(shell.selectedSection == .topics)
        shell.connections.selection = b.id
        #expect(shell.selectedSection == nil)
        shell.connections.selection = a.id
        #expect(shell.selectedSection == .topics)
    }

    @Test func settingASectionWithoutAConnectionIsIgnored() async {
        let shell = await makeShell([profile("a")])
        shell.selectedSection = .groups
        #expect(shell.selectedSection == nil)
    }

    @Test func deletedConnectionFallsBackToNone() async {
        let a = profile("a")
        let shell = await makeShell([a])
        shell.connections.selection = a.id
        shell.selectedSection = .groups
        await shell.connections.delete(id: a.id)
        shell.pruneSelection()
        #expect(shell.connections.selection == nil)
        #expect(shell.selectedConnection == nil)
        #expect(shell.selectedSection == nil)
    }

    @Test func pruneKeepsSelectionWhileListIsNotLoaded() {
        let model = ConnectionsViewModel(store: InMemoryConnectionStore(), secrets: InMemorySecretStore())
        let shell = ShellViewModel(connections: model)
        let id = UUID()
        model.selection = id
        shell.pruneSelection()
        #expect(model.selection == id)
    }

    @Test func topicListIsSharedPerSessionAndResetByProfileEdits() async {
        let a = profile("a")
        let shell = await makeShell([a])
        let registry = ClusterSessions(secrets: InMemorySecretStore(), factory: { _ in FakeKafkaClient() })
        let session = await registry.session(for: a)
        let first = shell.topicList(for: session)
        #expect(shell.topicList(for: session) === first)

        var edited = a
        edited.bootstrapServers = "other:9092"
        await session.updateConnection(edited)
        #expect(shell.topicList(for: session) !== first)
    }

    @Test func topicListIsDroppedWithItsConnection() async {
        let a = profile("a")
        let shell = await makeShell([a])
        let registry = ClusterSessions(secrets: InMemorySecretStore(), factory: { _ in FakeKafkaClient() })
        let session = await registry.session(for: a)
        let first = shell.topicList(for: session)
        await shell.connections.delete(id: a.id)
        shell.pruneSelection()
        #expect(shell.topicList(for: session) !== first)
    }

    @Test func sectionsHaveDistinctTitlesAndPrompts() {
        #expect(Set(ShellSection.allCases.map(\.title)).count == 2)
        #expect(Set(ShellSection.allCases.map(\.detailPrompt)).count == 2)
    }
}

@Suite("Session status presentation")
struct SessionStatusPresentationTests {
    typealias Status = ClusterSession.Status

    @Test func everyStateHasTextAndASymbol() {
        let all: [Status] = [.idle, .connecting, .connected, .failed(message: "x")]
        #expect(Set(all.map(\.label)).count == all.count)
        #expect(Set(all.map(\.symbolName)).count == all.count)
    }

    @Test func actionMatchesState() {
        #expect(Status.idle.action == .connect)
        #expect(Status.failed(message: "x").action == .connect)
        #expect(Status.connecting.action == .disconnect)
        #expect(Status.connected.action == .disconnect)
    }
}

@Suite("EnvironmentBannerPresentation")
struct EnvironmentBannerPresentationTests {
    private func banner(_ environment: ClusterConnection.Environment, readOnly: Bool) -> EnvironmentBannerPresentation {
        EnvironmentBannerPresentation(clusterName: "prod-eu", environment: environment, isReadOnly: readOnly)
    }

    @Test func toneAndEmphasisFollowTheEnvironment() {
        #expect(banner(.local, readOnly: false).tone == .calm)
        #expect(banner(.staging, readOnly: false).tone == .caution)
        #expect(banner(.production, readOnly: true).tone == .danger)
        #expect(banner(.production, readOnly: true).isEmphasized)
        #expect(!banner(.local, readOnly: false).isEmphasized)
        #expect(!banner(.staging, readOnly: false).isEmphasized)
    }

    @Test func environmentsDifferInTextAndSymbol() {
        let all = ClusterConnection.Environment.allCases.map { banner($0, readOnly: false) }
        #expect(Set(all.map(\.environmentLabel)).count == all.count)
        #expect(Set(all.map(\.environmentSymbol)).count == all.count)
    }

    @Test func readOnlyHasItsOwnTextAndSymbol() {
        let on = banner(.local, readOnly: true), off = banner(.local, readOnly: false)
        #expect(on.readOnlyLabel != off.readOnlyLabel)
        #expect(on.readOnlySymbol != off.readOnlySymbol)
        #expect(on.toggleTitle != off.toggleTitle)
    }

    @Test func onlyAllowingWritesOnProductionNeedsConfirmation() {
        #expect(banner(.production, readOnly: true).toggleNeedsConfirmation)
        #expect(!banner(.production, readOnly: false).toggleNeedsConfirmation)
        #expect(!banner(.local, readOnly: true).toggleNeedsConfirmation)
        #expect(!banner(.staging, readOnly: true).toggleNeedsConfirmation)
        #expect(!banner(.local, readOnly: false).toggleNeedsConfirmation)
    }

    @Test func confirmationNamesTheClusterAndAccessibilityIsOneLabel() {
        let p = banner(.production, readOnly: true)
        #expect(p.confirmationTitle.contains("prod-eu"))
        #expect(p.confirmationMessage.contains("prod-eu"))
        #expect(p.accessibilityLabel.contains("prod-eu"))
        #expect(p.accessibilityLabel.contains(p.environmentLabel))
        #expect(p.accessibilityLabel.contains(p.readOnlyLabel))
    }
}
