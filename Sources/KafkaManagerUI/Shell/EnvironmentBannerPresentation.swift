import Foundation
import KafkaManagerCore

/// Everything the environment banner says, decided without a view. Color comes from `tone`; the text and symbols
/// carry the same information so color is never the only signal.
public struct EnvironmentBannerPresentation: Sendable, Equatable {
    public let clusterName: String
    public let environment: ClusterConnection.Environment
    public let isReadOnly: Bool

    public init(clusterName: String, environment: ClusterConnection.Environment, isReadOnly: Bool) {
        self.clusterName = clusterName
        self.environment = environment
        self.isReadOnly = isReadOnly
    }

    public var environmentLabel: String { environment.label }
    /// First line of the banner: which cluster, in which environment.
    public var title: String { String(localized: "\(clusterName) · \(environmentLabel)") }
    public var environmentSymbol: String { environment.symbolName }
    public var tone: EnvironmentTone { environment.tone }
    /// Production is drawn heavier: a solid fill instead of a tint.
    public var isEmphasized: Bool { environment == .production }

    public var readOnlyLabel: String {
        isReadOnly ? String(localized: "Read-only") : String(localized: "Read-write")
    }

    public var readOnlySymbol: String { isReadOnly ? "lock.fill" : "lock.open" }

    /// The toggle offers the opposite of the current mode.
    public var toggleTitle: String {
        isReadOnly ? String(localized: "Allow Writes…") : String(localized: "Make Read-only")
    }

    /// Allowing writes on Production must be confirmed; every other change applies at once.
    public var toggleNeedsConfirmation: Bool { isReadOnly && environment == .production }

    public var confirmationTitle: String {
        String(localized: "Allow writes on \(clusterName)?")
    }

    public var confirmationMessage: String {
        String(localized: "\(clusterName) is a Production cluster. While writes are allowed, topics can be created and deleted. This lasts until you disconnect or switch back to read-only.")
    }

    public var confirmationButtonTitle: String { String(localized: "Allow Writes") }

    public var accessibilityLabel: String {
        String(localized: "\(clusterName), \(environmentLabel) environment, \(readOnlyLabel)")
    }
}
