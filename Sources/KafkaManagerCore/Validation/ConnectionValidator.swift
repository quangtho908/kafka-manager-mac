import Foundation

/// Form-level checks for a connection profile. Pure and synchronous: no I/O, no DNS.
public enum ConnectionValidator {
    public static let maxNameLength = 64

    /// Returns every issue at once, ordered by field (name, bootstrap servers, user name, SASL mechanism) and then by
    /// entry order. An empty array is valid; warnings alone still allow saving.
    /// - Parameter existingNames: names of the other profiles; leave out the profile being edited.
    public static func validate(
        name: String,
        bootstrapServers: String,
        security: ClusterConnection.Security,
        saslMechanism: ClusterConnection.SASLMechanism?,
        username: String?,
        existingNames: [String]
    ) -> [ValidationIssue] {
        var issues = nameIssues(name, existingNames: existingNames)
        switch BootstrapServers.parse(bootstrapServers) {
        case .success(let servers): issues += servers.warnings
        case .failure(let failure): issues += failure.issues
        }
        if security == .saslPlaintext || security == .saslSsl {
            if username?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ?? true {
                issues.append(ValidationIssue(field: .username, severity: .error,
                    message: String(localized: "Enter a user name; this security setting requires SASL login.")))
            }
            if saslMechanism == nil {
                issues.append(ValidationIssue(field: .saslMechanism, severity: .error,
                    message: String(localized: "Choose a SASL mechanism; this security setting requires SASL login.")))
            }
        }
        return issues
    }

    private static func nameIssues(_ name: String, existingNames: [String]) -> [ValidationIssue] {
        let trimmed = name.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty {
            return [ValidationIssue(field: .name, severity: .error, message: String(localized: "Enter a name for this connection."))]
        }
        if trimmed.count > maxNameLength {
            return [ValidationIssue(field: .name, severity: .error,
                message: String(localized: "The name is too long (\(trimmed.count) characters); the limit is \(maxNameLength)."))]
        }
        let taken = existingNames.contains {
            $0.trimmingCharacters(in: .whitespacesAndNewlines).caseInsensitiveCompare(trimmed) == .orderedSame
        }
        guard taken else { return [] }
        return [ValidationIssue(field: .name, severity: .warning,
            message: String(localized: "Another connection is already named \"\(trimmed)\". You can still save, but they will be hard to tell apart."))]
    }
}
