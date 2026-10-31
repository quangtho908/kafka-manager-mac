import Foundation

/// Why a topic name is rejected. Pure syntax checks, Kafka's own rules; no broker call.
public enum TopicNameProblem: Hashable, Sendable {
    case empty
    case reserved(String)
    /// The distinct offending characters, in order of first appearance.
    case invalidCharacters([Character])
    case tooLong(count: Int)

    /// English, safe to show. Never echoes more than the offending characters.
    public var message: String {
        switch self {
        case .empty:
            String(localized: "Enter a topic name.")
        case .reserved(let name):
            String(localized: "\"\(name)\" isn't allowed as a topic name.")
        case .invalidCharacters(let characters):
            String(localized: "Use only letters, digits, \".\", \"_\" and \"-\". Not allowed: \(String(characters)).")
        case .tooLong(let count):
            String(localized: "The name is \(count) characters long; the limit is \(TopicNameValidator.maxLength).")
        }
    }
}

public enum TopicNameValidator {
    public static let maxLength = 249

    /// The first problem found, or nil for a valid name. The name is checked exactly as given: no trimming, so a
    /// leading or trailing space is an invalid character. Only ASCII letters and digits, ".", "_" and "-" pass.
    public static func problem(in name: String) -> TopicNameProblem? {
        if name.isEmpty { return .empty }
        if name == "." || name == ".." { return .reserved(name) }
        var seen = Set<Character>()
        var invalid: [Character] = []
        for character in name where !isAllowed(character) && seen.insert(character).inserted {
            invalid.append(character)
        }
        if !invalid.isEmpty { return .invalidCharacters(invalid) }
        // Every character is ASCII here, so the count is also the byte length the broker checks.
        if name.count > maxLength { return .tooLong(count: name.count) }
        return nil
    }

    /// Kafka maps "." to "_" in metric names, so "a.b" and "a_b" collide there. Allowed, but worth a warning.
    public static func hasMetricNameCollisionRisk(_ name: String) -> Bool {
        name.contains(".") && name.contains("_")
    }

    private static func isAllowed(_ character: Character) -> Bool {
        guard character.isASCII else { return false }
        return character.isLetter || character.isNumber || character == "." || character == "_" || character == "-"
    }
}
