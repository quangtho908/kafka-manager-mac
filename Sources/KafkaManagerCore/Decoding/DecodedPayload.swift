import Foundation

/// The "Value" tabs of the message detail.
public enum PayloadFormat: Sendable, Hashable, CaseIterable {
    case auto, json, text, hex
}

/// What a payload turned out to be.
public enum PayloadKind: Sendable, Hashable {
    /// No bytes at all (a tombstone value, or a missing key).
    case null
    /// Zero bytes.
    case empty
    case json
    case text
    /// A hex dump: asked for, or the fallback for bytes that are not text.
    case hex
}

/// Which part of a message is being decoded; only the label of a null payload differs.
public enum PayloadRole: Sendable, Hashable {
    case key, value
}

public struct DecodedPayload: Sendable, Equatable {
    public let kind: PayloadKind
    /// What to show. At most the requested limit of the payload, so it may be a prefix; see `isTruncated`.
    public let displayText: String
    /// Size of the whole payload, not of the display text.
    public let byteCount: Int
    /// The display text covers only part of the payload.
    public let isTruncated: Bool
    /// Plain-language remarks to show beside the payload ("Invalid UTF-8; shown as hex"). Never contain payload bytes.
    public let notes: [String]

    public init(kind: PayloadKind, displayText: String, byteCount: Int, isTruncated: Bool = false, notes: [String] = []) {
        self.kind = kind
        self.displayText = displayText
        self.byteCount = byteCount
        self.isTruncated = isTruncated
        self.notes = notes
    }
}
