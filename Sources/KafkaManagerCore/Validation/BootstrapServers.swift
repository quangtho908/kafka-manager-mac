import Foundation
import Darwin

/// Why a bootstrap list was rejected: every issue found, errors and warnings, in entry order.
public struct BootstrapServersError: Error, Hashable, Sendable {
    public let issues: [ValidationIssue]
}

/// A parsed, normalized `host[:port],host[:port]` list. Pure syntax checks only: no DNS, no reachability.
public struct BootstrapServers: Hashable, Sendable {
    public struct Entry: Hashable, Sendable, CustomStringConvertible {
        /// IPv6 literals are stored without brackets.
        public let host: String
        public let port: Int

        public var description: String { host.contains(":") ? "[\(host)]:\(port)" : "\(host):\(port)" }
    }

    public static let defaultPort = 9092
    public static let maxEntries = 20

    private static let maxPort = 65_535
    private static let maxPortDigits = 5
    private static let maxHostLength = 253
    private static let maxLabelLength = 63
    private static let maxOctetDigits = 3
    private static let octetCount = 4
    private static let maxOctet = 255

    public let entries: [Entry]
    /// Non-blocking findings from a successful parse (dropped duplicates).
    public let warnings: [ValidationIssue]

    /// "host1:9092,host2:9093": trimmed, default port filled in, order kept, duplicates removed.
    public var normalized: String { entries.map(\.description).joined(separator: ",") }

    public static func parse(_ text: String) -> Result<BootstrapServers, BootstrapServersError> {
        if text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return .failure(BootstrapServersError(issues: [error(String(localized: "Enter at least one broker address, like localhost:9092."))]))
        }
        let pieces = text.split(separator: ",", omittingEmptySubsequences: false)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }

        var issues: [ValidationIssue] = []
        var entries: [Entry] = []
        var seen = Set<String>()
        var hasError = false
        if pieces.count > maxEntries {
            issues.append(error(String(localized: "Too many broker addresses (\(pieces.count)); the limit is \(maxEntries).")))
            hasError = true
        }
        for (offset, piece) in pieces.enumerated() {
            switch parseEntry(piece, position: offset + 1) {
            case .failure(let issue):
                issues.append(issue)
                hasError = true
            case .success(let entry):
                if seen.insert("\(entry.host.lowercased()):\(entry.port)").inserted {
                    entries.append(entry)
                } else {
                    issues.append(ValidationIssue(field: .bootstrapServers, severity: .warning,
                        message: String(localized: "\"\(piece)\" is listed more than once; the duplicate is ignored.")))
                }
            }
        }
        return hasError ? .failure(BootstrapServersError(issues: issues)) : .success(BootstrapServers(entries: entries, warnings: issues))
    }

    // MARK: Entry parsing

    private static func error(_ message: String) -> ValidationIssue {
        ValidationIssue(field: .bootstrapServers, severity: .error, message: message)
    }

    private static func parseEntry(_ entry: String, position: Int) -> Result<Entry, ValidationIssue> {
        func fail(_ message: String) -> Result<Entry, ValidationIssue> { .failure(error(message)) }

        if entry.isEmpty { return fail(String(localized: "Address \(position) is empty. Remove the extra comma.")) }
        // Checked first and never echoed: the entry may contain a password.
        if entry.contains("@") {
            return fail(String(localized: "Address \(position) contains \"@\". Remove the user name and password; they are set separately."))
        }
        if entry.contains(where: \.isWhitespace) {
            return fail(String(localized: "\"\(entry)\" contains a space. Separate addresses with commas."))
        }
        if entry.contains("://") {
            return fail(String(localized: "\"\(entry)\" includes a protocol. Use just host:port."))
        }
        if entry.contains(where: { "/?#".contains($0) }) {
            return fail(String(localized: "\"\(entry)\" includes a path or query. Use just host:port."))
        }

        let hostText: String
        let portText: String?
        if entry.hasPrefix("[") {
            guard let close = entry.firstIndex(of: "]") else {
                return fail(String(localized: "\"\(entry)\" is missing a closing \"]\"."))
            }
            hostText = String(entry[entry.index(after: entry.startIndex)..<close])
            let rest = entry[entry.index(after: close)...]
            if rest.isEmpty {
                portText = nil
            } else if rest.hasPrefix(":") {
                portText = String(rest.dropFirst())
            } else {
                return fail(String(localized: "\"\(entry)\" has unexpected text after \"]\"."))
            }
            guard isIPv6(hostText) else {
                return fail(String(localized: "\"\(entry)\" is not a valid IPv6 address."))
            }
        } else {
            let parts = entry.split(separator: ":", omittingEmptySubsequences: false)
            if parts.count > 2 {
                return fail(String(localized: "\"\(entry)\" looks like an IPv6 address. Put it in brackets, like [::1]:9092."))
            }
            hostText = String(parts[0])
            portText = parts.count == 2 ? String(parts[1]) : nil
            if let problem = hostProblem(hostText) {
                return fail(hostText.isEmpty
                    ? String(localized: "\"\(entry)\" is missing a host.")
                    : String(localized: "\"\(entry)\": \(problem)"))
            }
        }

        guard let port = port(from: portText) else {
            return fail(String(localized: "\"\(entry)\" has an invalid port. Use a number from 1 to \(maxPort)."))
        }
        return .success(Entry(host: hostText, port: port))
    }

    /// nil text means "no port given" and yields the default; present text must be 1...65535 digits only.
    private static func port(from text: String?) -> Int? {
        guard let text else { return defaultPort }
        guard !text.isEmpty, text.count <= maxPortDigits, text.allSatisfy({ $0.isASCII && $0.isNumber }),
              let value = Int(text), (1...maxPort).contains(value) else { return nil }
        return value
    }

    private static func isIPv6(_ text: String) -> Bool {
        var address = in6_addr()
        return inet_pton(AF_INET6, text, &address) == 1
    }

    /// nil when `host` is a valid IPv4 dotted quad or hostname. A host made only of digits and dots must be a
    /// valid IPv4 address, so "999.1.1.1" and "1.2.3" are rejected rather than treated as hostnames.
    private static func hostProblem(_ host: String) -> String? {
        if host.isEmpty { return String(localized: "missing host") }
        let labels = host.split(separator: ".", omittingEmptySubsequences: false)
        if host.allSatisfy({ $0.isASCII && ($0.isNumber || $0 == ".") }) {
            return isIPv4(labels) ? nil : String(localized: "not a valid IPv4 address")
        }
        if host.count > maxHostLength { return String(localized: "host name is longer than \(maxHostLength) characters") }
        for label in labels {
            if label.isEmpty { return String(localized: "host name has an empty part (check the dots)") }
            if label.count > maxLabelLength { return String(localized: "a part of the host name is longer than \(maxLabelLength) characters") }
            if label.hasPrefix("-") || label.hasSuffix("-") { return String(localized: "parts of a host name can't start or end with \"-\"") }
            if !label.allSatisfy({ $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "-" || $0 == "_") }) {
                return String(localized: "host name has an invalid character")
            }
        }
        if labels.last?.allSatisfy({ $0.isASCII && $0.isNumber }) == true {
            return String(localized: "not a valid IPv4 address")
        }
        return nil
    }

    private static func isIPv4(_ labels: [Substring]) -> Bool {
        labels.count == octetCount && labels.allSatisfy { label in
            !label.isEmpty && label.count <= maxOctetDigits && (Int(label).map { $0 <= maxOctet } ?? false)
        }
    }
}
