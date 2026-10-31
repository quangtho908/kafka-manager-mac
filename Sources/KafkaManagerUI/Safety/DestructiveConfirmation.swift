import Foundation
import KafkaManagerCore

/// What a destructive action must show before it runs: the cluster, the object, the consequences, and the exact
/// name the user has to type. Pure data; the sheet draws it.
public struct DestructiveConfirmation: Sendable, Identifiable {
    public let id = UUID()
    public let title: String
    public let clusterName: String
    public let environment: ClusterConnection.Environment
    /// "topic", "consumer group": used in the field's label.
    public let objectKind: String
    /// The text the user must type, character for character.
    public let objectName: String
    public let actionTitle: String
    public let consequences: String

    public init(
        title: String,
        clusterName: String,
        environment: ClusterConnection.Environment,
        objectKind: String,
        objectName: String,
        actionTitle: String,
        consequences: String
    ) {
        self.title = title
        self.clusterName = clusterName
        self.environment = environment
        self.objectKind = objectKind
        self.objectName = objectName
        self.actionTitle = actionTitle
        self.consequences = consequences
    }

    public var showsProductionBanner: Bool { environment == .production }

    /// True only when `typed` is exactly `objectName`: case-sensitive, no trimming (a stray space is a mismatch),
    /// and compared by Unicode scalars, so canonically-equivalent spellings do not match each other the way
    /// Swift's `==` would let them. An empty name is never satisfied.
    public func isSatisfied(by typed: String) -> Bool {
        !objectName.isEmpty && typed.unicodeScalars.elementsEqual(objectName.unicodeScalars)
    }

    public var fieldLabel: String {
        String(localized: "Type the \(objectKind) name to confirm")
    }

    public var accessibilitySummary: String {
        String(localized: "\(title). \(objectKind) \(objectName) on \(clusterName), \(environment.label) environment.")
    }
}

/// Whether a write action may be offered, from the session's read-only flag. The Core guard still enforces it;
/// this only keeps the UI from offering what would be refused.
public struct WriteGate: Sendable, Equatable {
    public let isReadOnly: Bool

    public init(isReadOnly: Bool) {
        self.isReadOnly = isReadOnly
    }

    public var allowsWrites: Bool { !isReadOnly }

    /// Tooltip for the disabled control; nil when writes are allowed.
    public var disabledReason: String? {
        isReadOnly ? String(localized: "This cluster is read-only") : nil
    }

    /// For `.help(_:)`, which takes a string: empty when there is nothing to explain.
    public var help: String { disabledReason ?? "" }
}
