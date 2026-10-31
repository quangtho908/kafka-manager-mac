import Foundation
import KafkaManagerCore

/// How loudly an environment should be drawn. The view maps a tone to a system color; the label text always
/// accompanies it so color is never the only signal.
public enum EnvironmentTone: Sendable, Equatable {
    case calm, caution, danger
}

public extension ClusterConnection.Environment {
    var label: String {
        switch self {
        case .local: String(localized: "Local")
        case .staging: String(localized: "Staging")
        case .production: String(localized: "Production")
        }
    }

    var tone: EnvironmentTone {
        switch self {
        case .local: .calm
        case .staging: .caution
        case .production: .danger
        }
    }

    var symbolName: String {
        switch self {
        case .local: "desktopcomputer"
        case .staging: "exclamationmark.triangle"
        case .production: "exclamationmark.octagon.fill"
        }
    }
}

public extension ClusterConnection.Security {
    var label: String {
        switch self {
        case .plaintext: String(localized: "Plaintext")
        case .ssl: String(localized: "SSL")
        case .saslPlaintext: String(localized: "SASL_PLAINTEXT")
        case .saslSsl: String(localized: "SASL_SSL")
        }
    }
}

public extension ClusterConnection.SASLMechanism {
    var label: String {
        switch self {
        case .plain: String(localized: "PLAIN")
        case .scramSha256: String(localized: "SCRAM-SHA-256")
        case .scramSha512: String(localized: "SCRAM-SHA-512")
        }
    }
}

/// One-line description of a bootstrap list for a sidebar row, like "broker-1:9092 +2 more".
public enum HostSummary {
    public static func make(_ bootstrapServers: String) -> String {
        if case .success(let servers) = BootstrapServers.parse(bootstrapServers), let first = servers.entries.first {
            let others = servers.entries.count - 1
            return others > 0 ? String(localized: "\(first.description) +\(others) more") : first.description
        }
        // Unparseable text (hand-edited database): show the first piece as-is rather than hiding the row.
        let first = bootstrapServers.split(separator: ",").first.map(String.init) ?? bootstrapServers
        return first.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}

public extension [ValidationIssue] {
    /// Issues for one field, in reported order.
    func issues(for field: ValidationIssue.Field) -> [ValidationIssue] {
        filter { $0.field == field }
    }

    /// Errors only appear once the user has touched their field (an empty new form shouldn't start out red);
    /// warnings come from content the user already typed, so they always show.
    func visible(touched: Set<ValidationIssue.Field>) -> [ValidationIssue] {
        filter { $0.severity == .warning || touched.contains($0.field) }
    }
}

/// What the delete confirmation asks for: any profile needs a click, production also needs its name typed.
public struct DeleteConfirmation: Sendable, Equatable {
    public let connection: ClusterConnection

    public init(connection: ClusterConnection) {
        self.connection = connection
    }

    public var requiresTypedName: Bool {
        connection.environment == .production
    }

    public func isSatisfied(by typedName: String) -> Bool {
        !requiresTypedName || typedName.trimmingCharacters(in: .whitespacesAndNewlines) == connection.name
    }
}

extension DeleteConfirmation: Identifiable {
    public var id: UUID { connection.id }
}

/// A request to show the connection form, so a menu command and the list can share one sheet.
public struct ConnectionFormRequest: Identifiable, Sendable, Equatable {
    public let id = UUID()
    public let mode: ConnectionFormViewModel.Mode

    public init(mode: ConnectionFormViewModel.Mode) {
        self.mode = mode
    }
}
