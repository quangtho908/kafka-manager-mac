import Foundation
import KafkaManagerCore

public struct MessageID: Hashable, Sendable {
    public let partition: Int
    public let offset: Int64
}

/// A fetched message plus the short strings the table needs, computed once when it arrives. The full payload decode
/// happens only for the selected row.
public struct MessageRow: Identifiable, Sendable, Equatable {
    public var id: MessageID { MessageID(partition: message.partition, offset: message.offset) }
    public let message: KafkaMessage
    public let keyPreview: PayloadPreview
    public let valuePreview: PayloadPreview
    /// Key plus value bytes.
    public let size: Int

    public init(_ message: KafkaMessage) {
        self.message = message
        keyPreview = PayloadDecoder.previewParts(message.key, role: .key)
        valuePreview = PayloadDecoder.previewParts(message.value, role: .value)
        size = (message.key?.count ?? 0) + (message.value?.count ?? 0)
    }

    public var isTombstone: Bool { message.value == nil }

    /// Rows are immutable once fetched, so identity is equality. SwiftUI compares the whole table's rows on every
    /// update; the default memberwise comparison would walk every payload.
    public static func == (lhs: MessageRow, rhs: MessageRow) -> Bool { lhs.id == rhs.id }
}

public enum MessageSortKey: Sendable, Hashable, CaseIterable {
    case partition, offset, timestamp, size
}

/// Pure sort and filter over fetched rows.
public enum MessageRows {
    /// Ties fall back to partition then offset, so the order is total.
    nonisolated public static func sort(_ rows: [MessageRow], by key: MessageSortKey, ascending: Bool) -> [MessageRow] {
        rows.sorted { lhs, rhs in
            let order: Int
            switch key {
            case .partition: order = compare(lhs.message.partition, rhs.message.partition)
            case .offset: order = compare(lhs.message.offset, rhs.message.offset)
            case .timestamp: order = compare(lhs.message.timestamp, rhs.message.timestamp)
            case .size: order = compare(lhs.size, rhs.size)
            }
            if order != 0 { return ascending ? order < 0 : order > 0 }
            return (lhs.message.partition, lhs.message.offset) < (rhs.message.partition, rhs.message.offset)
        }
    }

    /// Case-insensitive substring of the key or value *preview text* (the first 10 KB of each, as shown; for binary
    /// payloads the hex run). The size suffix is not searched. Whitespace around the search is ignored.
    nonisolated public static func filter(_ rows: [MessageRow], search: String) -> [MessageRow] {
        let needle = search.trimmingCharacters(in: .whitespaces)
        guard !needle.isEmpty else { return rows }
        return rows.filter { row in
            row.keyPreview.text.range(of: needle, options: .caseInsensitive) != nil
                || row.valuePreview.text.range(of: needle, options: .caseInsensitive) != nil
        }
    }

    private nonisolated static func compare<T: Comparable>(_ a: T, _ b: T) -> Int { a < b ? -1 : (a > b ? 1 : 0) }
}
