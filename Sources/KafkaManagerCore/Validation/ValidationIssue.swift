import Foundation

public struct ValidationIssue: Hashable, Sendable, Error {
    /// Declaration order is the order issues are reported in.
    public enum Field: Int, Hashable, Sendable, Comparable {
        case name, bootstrapServers, username, saslMechanism

        public static func < (lhs: Field, rhs: Field) -> Bool { lhs.rawValue < rhs.rawValue }
    }

    public enum Severity: Hashable, Sendable {
        /// Blocks saving.
        case error
        /// Worth showing, but saving is allowed.
        case warning
    }

    public let field: Field
    public let severity: Severity
    /// English, safe to show; never contains a secret.
    public let message: String

    public init(field: Field, severity: Severity, message: String) {
        self.field = field
        self.severity = severity
        self.message = message
    }
}
