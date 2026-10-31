import Foundation

/// SASL credentials handed to the client at connect time. Deliberately not Codable and
/// redacted in every textual form so the password cannot leak through logs or dumps.
public struct Credentials: Hashable, Sendable {
    public let username: String
    public let password: String

    public init(username: String, password: String) {
        self.username = username
        self.password = password
    }
}

extension Credentials: CustomStringConvertible, CustomDebugStringConvertible, CustomReflectable {
    private static let redacted = "<redacted>"

    public var description: String { "Credentials(username: \(username), password: \(Self.redacted))" }
    public var debugDescription: String { description }

    public var customMirror: Mirror {
        Mirror(self, children: ["username": username, "password": Self.redacted], displayStyle: .struct)
    }
}
