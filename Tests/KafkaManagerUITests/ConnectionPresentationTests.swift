import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

@Suite("Connection presentation")
struct ConnectionPresentationTests {
    @Test func environmentsMapToDistinctTonesAndLabels() {
        #expect(ClusterConnection.Environment.local.tone == .calm)
        #expect(ClusterConnection.Environment.staging.tone == .caution)
        #expect(ClusterConnection.Environment.production.tone == .danger)
        let labels = ClusterConnection.Environment.allCases.map(\.label)
        #expect(Set(labels).count == labels.count)
        #expect(labels.allSatisfy { !$0.isEmpty })
        let symbols = ClusterConnection.Environment.allCases.map(\.symbolName)
        #expect(Set(symbols).count == symbols.count)
    }

    @Test func securityAndMechanismLabelsAreUnique() {
        let security = ClusterConnection.Security.allCases.map(\.label)
        #expect(Set(security).count == security.count)
        let mechanisms = ClusterConnection.SASLMechanism.allCases.map(\.label)
        #expect(mechanisms == ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
    }

    @Test(arguments: [
        ("localhost:9092", "localhost:9092"),
        ("localhost", "localhost:9092"),
        ("b1:9092,b2:9092,b3:9093", "b1:9092 +2 more"),
        ("b1, b2", "b1:9092 +1 more"),
        ("b1,b1:9092", "b1:9092"),
        ("[::1]:9092", "[::1]:9092"),
        ("  bad host ,x", "bad host"),
        ("", ""),
    ])
    func hostSummary(input: String, expected: String) {
        #expect(HostSummary.make(input) == expected)
    }

    @Test func issuesFilterByFieldAndTouched() {
        let issues = ConnectionValidator.validate(
            name: "", bootstrapServers: "", security: .saslSsl, saslMechanism: nil, username: nil, existingNames: [])
        #expect(issues.issues(for: .name).count == 1)
        #expect(issues.issues(for: .saslMechanism).count == 1)
        #expect(issues.visible(touched: []).isEmpty)
        #expect(issues.visible(touched: [.name, .username]).map(\.field) == [.name, .username])
    }

    @Test func warningsAreVisibleEvenUntouched() {
        let issues = ConnectionValidator.validate(
            name: "a", bootstrapServers: "x,x", security: .plaintext, saslMechanism: nil, username: nil, existingNames: ["A"])
        #expect(issues.visible(touched: []).count == 2)
    }

    @Test func nonProductionDeleteNeedsNoTyping() {
        let confirmation = DeleteConfirmation(connection: ClusterConnection(name: "dev", bootstrapServers: "h", environment: .staging))
        #expect(!confirmation.requiresTypedName)
        #expect(confirmation.isSatisfied(by: ""))
    }

    @Test func productionDeleteNeedsTheExactName() {
        let confirmation = DeleteConfirmation(connection: ClusterConnection(name: "Prod EU", bootstrapServers: "h", environment: .production))
        #expect(confirmation.requiresTypedName)
        #expect(!confirmation.isSatisfied(by: ""))
        #expect(!confirmation.isSatisfied(by: "prod eu"))
        #expect(!confirmation.isSatisfied(by: "Prod"))
        #expect(confirmation.isSatisfied(by: "Prod EU"))
        #expect(confirmation.isSatisfied(by: "  Prod EU \n"))
    }

    @MainActor @Test func makeFormViewModelExcludesTheEditedName() async {
        let a = ClusterConnection(name: "a", bootstrapServers: "h"), b = ClusterConnection(name: "b", bootstrapServers: "h")
        let model = ConnectionsViewModel(store: InMemoryConnectionStore(connections: [a, b]), secrets: InMemorySecretStore())
        await model.load()
        let editing = model.makeFormViewModel(mode: .edit(a))
        #expect(editing.connectionID == a.id)
        let duplicate = model.makeFormViewModel(mode: .duplicate(a))
        #expect(duplicate.issues.issues(for: .name).first?.severity != .error)
        let fresh = model.makeFormViewModel(mode: .new)
        fresh.name = "A"
        fresh.bootstrapServers = "h"
        #expect(fresh.issues.issues(for: .name).first?.severity == .warning)
        editing.bootstrapServers = "h"
        editing.name = "a"
        #expect(editing.issues.isEmpty)
    }

    @MainActor @Test func makeFormViewModelReturnsIndependentInstances() {
        let model = ConnectionsViewModel(store: InMemoryConnectionStore(), secrets: InMemorySecretStore())
        let first = model.makeFormViewModel(mode: .new)
        let second = model.makeFormViewModel(mode: .new)
        first.name = "typed"
        #expect(first !== second)
        #expect(second.name.isEmpty)
        #expect(first.connectionID != second.connectionID)
    }
}

@Suite("EnvironmentBadgeStyle")
struct EnvironmentBadgeStyleTests {
    @Test(arguments: [EnvironmentTone.calm, .caution, .danger])
    func normalBackgroundKeepsTheToneAndASelectedRowUsesThePrimaryStyle(tone: EnvironmentTone) {
        #expect(EnvironmentBadgeStyle.style(for: tone, onProminentBackground: false) == .tone(tone))
        #expect(EnvironmentBadgeStyle.style(for: tone, onProminentBackground: true) == .onSelection)
    }

    @Test func everyToneHasItsOwnColor() {
        let colors = [EnvironmentTone.calm, .caution, .danger].map { EnvironmentBadgeStyle.tone($0).color }
        #expect(Set(colors).count == 3)
        #expect(!colors.contains(EnvironmentBadgeStyle.onSelection.color))
    }
}
