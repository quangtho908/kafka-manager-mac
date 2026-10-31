import Foundation
import Observation
import KafkaManagerCore

public enum MessageDetailPart: Sendable, Hashable {
    case key, value
}

public enum MessageCopyKind: Sendable, Hashable {
    case key, value, message
}

/// What the Key or Value section shows right now.
public struct PayloadView: Sendable, Equatable {
    public var payload: DecodedPayload?
    /// A decode is running (only the slow path shows a spinner; small payloads decode in the same turn).
    public var isDecoding = false
    var lines: [DisplayLine] = []
    var wasSplit = false

    /// "Showing first 64 KB of 200 KB", or nil when everything is shown.
    public var truncationNote: String? {
        guard let payload, payload.isTruncated else { return nil }
        let shown = ByteCountFormatStyle(style: .file).format(Int64(PayloadDecoder.displayLimit))
        let total = ByteCountFormatStyle(style: .file).format(Int64(payload.byteCount))
        return String(localized: "Showing first \(shown) of \(total)")
    }
}

public struct MetadataRow: Identifiable, Sendable, Equatable {
    public let label: String
    public let value: String
    public var id: String { label }
}

public struct HeaderRow: Identifiable, Sendable, Equatable {
    public let id: Int
    public let key: String
    public let value: String
}

/// Decoded view of one selected message. Created per selection; decoding runs in tasks the view starts with
/// `.task(id:)`, so changing the selection or the format cancels the old work and a late result never lands.
@MainActor @Observable
public final class MessageDetailViewModel {
    public typealias Decode = @Sendable (Data?, PayloadFormat, PayloadRole, Int) async -> DecodedPayload

    public let topic: String
    public let message: KafkaMessage
    public var keyFormat = PayloadFormat.auto
    public var valueFormat = PayloadFormat.auto
    public var wordWrap = true
    public private(set) var key = PayloadView()
    public private(set) var value = PayloadView()
    /// What the last Copy button copied; cleared when a format changes.
    public private(set) var lastCopy: MessageCopyKind?
    public private(set) var isCopying = false

    @ObservationIgnored private let clipboard: any Clipboard
    @ObservationIgnored private let decode: Decode
    @ObservationIgnored private var generation: [MessageDetailPart: Int] = [:]

    /// The size above which decoding goes to `decodeAsync` and a spinner may show.
    public static let asyncThreshold = PayloadDecoder.displayLimit

    public init(topic: String, message: KafkaMessage, clipboard: any Clipboard, decode: @escaping Decode = MessageDetailViewModel.systemDecode) {
        self.topic = topic
        self.message = message
        self.clipboard = clipboard
        self.decode = decode
    }

    public static let systemDecode: Decode = { data, format, role, limit in
        await PayloadDecoder.decodeAsync(data, as: format, limit: limit, role: role)
    }

    // MARK: Decoding

    public func format(of part: MessageDetailPart) -> PayloadFormat { part == .key ? keyFormat : valueFormat }

    private func data(of part: MessageDetailPart) -> Data? { part == .key ? message.key : message.value }

    /// Decodes one part in the chosen format for display (first 64 KB). A newer call for the same part wins.
    public func load(_ part: MessageDetailPart) async {
        let id = (generation[part] ?? 0) + 1
        generation[part] = id
        let chosen = format(of: part)
        let payload = data(of: part)
        let role: PayloadRole = part == .key ? .key : .value
        let isSmall = (payload?.count ?? 0) <= Self.asyncThreshold

        if isSmall {
            // Same turn: no spinner, no flicker when stepping through rows.
            let result = PayloadDecoder.decode(payload, as: chosen, limit: PayloadDecoder.displayLimit, role: role)
            publish(result, for: part)
            return
        }
        update(part) { $0.isDecoding = true }
        let result = await decode(payload, chosen, role, PayloadDecoder.displayLimit)
        guard id == generation[part], !Task.isCancelled else { return }
        publish(result, for: part)
    }

    private func publish(_ payload: DecodedPayload, for part: MessageDetailPart) {
        var view = PayloadView(payload: payload, isDecoding: false)
        view.lines = DisplayLines.split(payload.displayText)
        view.wasSplit = DisplayLines.wasSplit(payload.displayText)
        if part == .key { key = view } else { value = view }
    }

    private func update(_ part: MessageDetailPart, _ change: (inout PayloadView) -> Void) {
        if part == .key { change(&key) } else { change(&value) }
    }

    public func setFormat(_ format: PayloadFormat, for part: MessageDetailPart) {
        if part == .key { keyFormat = format } else { valueFormat = format }
        lastCopy = nil
    }

    // MARK: Sections

    public var headerRows: [HeaderRow] {
        message.headers.enumerated().map { index, header in
            HeaderRow(id: index, key: header.key, value: PayloadDecoder.preview(header.value, role: .value))
        }
    }

    public var metadataRows: [MetadataRow] {
        [
            MetadataRow(label: String(localized: "Topic"), value: topic),
            MetadataRow(label: String(localized: "Partition"), value: String(message.partition)),
            MetadataRow(label: String(localized: "Offset"), value: String(message.offset)),
            MetadataRow(label: String(localized: "Timestamp"), value: Self.timestampText(message.timestamp)),
            MetadataRow(label: String(localized: "Key size"), value: Self.sizeText(message.key?.count)),
            MetadataRow(label: String(localized: "Value size"), value: Self.sizeText(message.value?.count)),
            MetadataRow(label: String(localized: "Headers"), value: String(message.headers.count)),
        ]
    }

    /// Local time with milliseconds and zone, then the epoch milliseconds: "2023-11-14T23:13:20.123+01:00 (1700000000123)".
    nonisolated public static func timestampText(_ date: Date, timeZone: TimeZone = .current) -> String {
        let millis = Int64((date.timeIntervalSince1970 * 1000).rounded())
        return "\(MessageExport.timestamp(date, timeZone: timeZone)) (\(millis))"
    }

    /// "none" for a null payload, otherwise "1,234 bytes".
    nonisolated public static func sizeText(_ count: Int?) -> String {
        guard let count else { return String(localized: "none") }
        return count == 1 ? String(localized: "1 byte") : String(localized: "\(count.formatted()) bytes")
    }

    // MARK: Copy

    public func canCopy(_ part: MessageDetailPart) -> Bool { data(of: part) != nil }

    /// Copies one part decoded in the chosen format, from the **full** bytes (not the 64 KB shown): Auto/JSON/Text
    /// give the same text as the panel for the whole payload, Hex gives the whole hex dump. A format that does not fit
    /// falls back exactly as the panel does. JSON larger than 32 MB is copied as text.
    public func copy(_ part: MessageDetailPart) async {
        guard let payload = data(of: part) else { return }
        isCopying = true
        defer { isCopying = false }
        let role: PayloadRole = part == .key ? .key : .value
        let result = await decode(payload, format(of: part), role, max(payload.count, 1))
        guard !Task.isCancelled else { return }
        clipboard.copy(result.displayText)
        lastCopy = part == .key ? .key : .value
    }

    /// Copies the whole message with `MessageExport.json`.
    public func copyMessage() {
        clipboard.copy(MessageExport.json(message, topic: topic))
        lastCopy = .message
    }
}
