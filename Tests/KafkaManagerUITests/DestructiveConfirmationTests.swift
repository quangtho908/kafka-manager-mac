import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private func confirmation(name: String = "orders.v1", environment: ClusterConnection.Environment = .production) -> DestructiveConfirmation {
    DestructiveConfirmation(
        title: "Delete topic?", clusterName: "prod-eu", environment: environment, objectKind: "topic",
        objectName: name, actionTitle: "Delete Topic", consequences: "Gone for good.")
}

@Suite("DestructiveConfirmation")
struct DestructiveConfirmationTests {
    @Test func exactNameSatisfies() {
        #expect(confirmation().isSatisfied(by: "orders.v1"))
    }

    @Test func emptyTypedTextNeverSatisfies() {
        #expect(!confirmation().isSatisfied(by: ""))
    }

    @Test func emptyObjectNameIsNeverSatisfied() {
        #expect(!confirmation(name: "").isSatisfied(by: ""))
    }

    @Test func matchingIsCaseSensitive() {
        #expect(!confirmation().isSatisfied(by: "Orders.v1"))
        #expect(!confirmation().isSatisfied(by: "ORDERS.V1"))
    }

    @Test func whitespaceIsNotTrimmed() {
        for typed in [" orders.v1", "orders.v1 ", "orders.v1\n", "\torders.v1", "orders .v1"] {
            #expect(!confirmation().isSatisfied(by: typed), "\(typed.debugDescription)")
        }
    }

    @Test func prefixesAndExtensionsDoNotMatch() {
        #expect(!confirmation().isSatisfied(by: "orders"))
        #expect(!confirmation().isSatisfied(by: "orders.v1.dlq"))
    }

    @Test func canonicallyEquivalentSpellingsDoNotMatch() {
        let composed = "caf\u{00E9}", decomposed = "cafe\u{0301}"
        #expect(composed == decomposed) // Swift's own == would accept this
        #expect(confirmation(name: composed).isSatisfied(by: composed))
        #expect(!confirmation(name: composed).isSatisfied(by: decomposed))
    }

    @Test func productionBannerOnlyForProduction() {
        #expect(confirmation(environment: .production).showsProductionBanner)
        #expect(!confirmation(environment: .staging).showsProductionBanner)
        #expect(!confirmation(environment: .local).showsProductionBanner)
    }

    @Test func fieldLabelAndSummaryNameTheObjectAndCluster() {
        let c = confirmation()
        #expect(c.fieldLabel.contains("topic"))
        #expect(c.accessibilitySummary.contains("orders.v1"))
        #expect(c.accessibilitySummary.contains("prod-eu"))
        #expect(c.accessibilitySummary.contains(ClusterConnection.Environment.production.label))
    }

    @Test func eachConfirmationHasItsOwnIdentity() {
        #expect(confirmation().id != confirmation().id)
    }
}

@Suite("WriteGate")
struct WriteGateTests {
    @Test func readOnlyBlocksWithAReason() {
        let gate = WriteGate(isReadOnly: true)
        #expect(!gate.allowsWrites)
        #expect(gate.disabledReason == "This cluster is read-only")
        #expect(gate.help == gate.disabledReason)
    }

    @Test func readWriteAllowsWithNoReason() {
        let gate = WriteGate(isReadOnly: false)
        #expect(gate.allowsWrites)
        #expect(gate.disabledReason == nil)
        #expect(gate.help.isEmpty)
    }
}
