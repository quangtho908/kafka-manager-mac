import Foundation
import Observation
import KafkaManagerCore

extension ProduceEncoding {
    public var title: String {
        switch self {
        case .text: String(localized: "Text")
        case .json: String(localized: "JSON")
        case .hex: String(localized: "Hex")
        }
    }

    /// The key is raw bytes or text; JSON only makes sense for a value.
    public static let keyChoices: [ProduceEncoding] = [.text, .hex]
}

/// One header row being edited.
public struct HeaderDraft: Identifiable, Equatable, Sendable {
    public enum Kind: String, CaseIterable, Sendable {
        case text, hex, null

        public var title: String {
            switch self {
            case .text: String(localized: "Text")
            case .hex: String(localized: "Hex")
            case .null: String(localized: "Null")
            }
        }
    }

    public let id: UUID
    public var key: String
    public var value: String
    public var kind: Kind

    public init(key: String = "", value: String = "", kind: Kind = .text, id: UUID = UUID()) {
        self.id = id
        self.key = key
        self.value = value
        self.kind = kind
    }

    /// A row the user has not touched is ignored rather than reported.
    var isBlank: Bool { key.isEmpty && value.isEmpty && kind != .null }
}

/// Where the form wants to show a problem.
public struct ProduceFormIssue: Hashable, Sendable {
    public enum Field: Hashable, Sendable { case partition, key, value, headers, timestamp, repeatCount }

    public let field: Field
    public let message: String
}

public struct ProduceSummary: Equatable, Sendable {
    public let sent: Int
    public let requested: Int
    public let last: ProduceResult

    /// "Sent to partition 2 at offset 41", or the count for a repeat.
    public var text: String {
        sent == 1
            ? String(localized: "Sent to partition \(last.partition) at offset \(last.offset)")
            : String(localized: "Sent \(sent) messages. The last went to partition \(last.partition) at offset \(last.offset).")
    }

    public var wasStopped: Bool { sent < requested }
}

/// The "Produce" sheet: what to send, checked as it is typed, and the send itself. Nothing leaves the app before Send
/// (and, on a production cluster, the typed topic-name confirmation).
@MainActor @Observable
public final class ProduceFormViewModel: Identifiable {
    public static let maxRepeat = 1000

    public enum SendState: Equatable, Sendable {
        case idle
        case sending
        case sent(ProduceSummary)
        case failed(message: String)
    }

    public nonisolated let id = UUID()
    public let topicName: String
    /// The partitions the picker offers, from the loaded topic detail.
    public let partitionIDs: [Int]
    /// Set when the form was filled from a message: where that message lives, shown as a note.
    public let originalPartition: Int?

    /// nil lets the partitioner choose.
    public var partition: Int?
    public var keyText = ""
    public var keyEncoding = ProduceEncoding.text
    public var valueText = ""
    public var valueEncoding = ProduceEncoding.text
    public var isTombstone = false
    public var headers: [HeaderDraft] = []
    public var useTimestamp = false
    public var timestamp = Date()
    public var repeatCount = 1

    public private(set) var state = SendState.idle
    /// (done, total) while a send runs.
    public private(set) var progress: (done: Int, total: Int)?
    public var pendingConfirmation: DestructiveConfirmation?

    @ObservationIgnored private let session: ClusterSession
    @ObservationIgnored private let onSent: @MainActor (ProduceResult) -> Void
    @ObservationIgnored private var sendTask: Task<Void, Never>?
    @ObservationIgnored private var cache: (inputs: Inputs, parsed: Parsed)?

    public init(
        session: ClusterSession, topicName: String, partitionIDs: [Int], prefill: KafkaMessage? = nil,
        onSent: @escaping @MainActor (ProduceResult) -> Void = { _ in }
    ) {
        self.session = session
        self.topicName = topicName
        self.partitionIDs = partitionIDs.sorted()
        self.onSent = onSent
        originalPartition = prefill?.partition
        if let prefill { fill(from: prefill) }
    }

    private func fill(from message: KafkaMessage) {
        if let key = message.key {
            let editable = ProduceEncoder.editableText(for: key)
            keyText = editable.text
            keyEncoding = editable.encoding
        }
        if let value = message.value {
            let editable = ProduceEncoder.editableText(for: value)
            valueText = editable.text
            valueEncoding = editable.encoding
        } else {
            isTombstone = true
        }
        headers = message.headers.map { header in
            guard let value = header.value else { return HeaderDraft(key: header.key, kind: .null) }
            let editable = ProduceEncoder.editableText(for: value)
            return HeaderDraft(key: header.key, value: editable.text, kind: editable.encoding == .hex ? .hex : .text)
        }
    }

    // MARK: Parsing

    private struct Inputs: Equatable {
        let partition: Int?
        let keyText: String
        let keyEncoding: ProduceEncoding
        let valueText: String
        let valueEncoding: ProduceEncoding
        let isTombstone: Bool
        let headers: [HeaderDraft]
        let useTimestamp: Bool
        let timestamp: Date
        let repeatCount: Int
    }

    private struct Parsed {
        var request: ProduceRequest?
        var issues: [ProduceFormIssue]
        var size: Int?
    }

    private var parsed: Parsed {
        let inputs = Inputs(
            partition: partition, keyText: keyText, keyEncoding: keyEncoding, valueText: valueText, valueEncoding: valueEncoding,
            isTombstone: isTombstone, headers: headers, useTimestamp: useTimestamp, timestamp: timestamp, repeatCount: repeatCount)
        if let cache, cache.inputs == inputs { return cache.parsed }
        let result = parse(inputs)
        cache = (inputs, result)
        return result
    }

    private func parse(_ inputs: Inputs) -> Parsed {
        var issues: [ProduceFormIssue] = []
        var failed = false

        var key: Data?
        if !inputs.keyText.isEmpty {
            switch ProduceEncoder.data(from: inputs.keyText, as: inputs.keyEncoding) {
            case .success(let data): key = data
            case .failure(let issue):
                failed = true
                issues.append(ProduceFormIssue(field: .key, message: issue.message))
            }
        }
        var value: Data?
        if !inputs.isTombstone {
            switch ProduceEncoder.data(from: inputs.valueText, as: inputs.valueEncoding) {
            case .success(let data): value = data
            case .failure(let issue):
                failed = true
                issues.append(ProduceFormIssue(field: .value, message: issue.message))
            }
        }
        var built: [MessageHeader] = []
        var rows: [Int] = []
        for (row, draft) in inputs.headers.enumerated() where !draft.isBlank {
            var headerValue: Data?
            switch draft.kind {
            case .null: headerValue = nil
            case .text: headerValue = Data(draft.value.utf8)
            case .hex:
                switch ProduceEncoder.data(from: draft.value, as: .hex) {
                case .success(let data): headerValue = data
                case .failure(let issue):
                    failed = true
                    issues.append(ProduceFormIssue(field: .headers, message: String(localized: "Header \(row + 1): \(issue.message)")))
                }
            }
            built.append(MessageHeader(key: draft.key, value: headerValue))
            rows.append(row)
        }
        if inputs.repeatCount < 1 || inputs.repeatCount > Self.maxRepeat {
            issues.append(ProduceFormIssue(field: .repeatCount, message: String(localized: "Repeat between 1 and \(Self.maxRepeat) times.")))
            failed = true
        }
        let request = ProduceRequest(
            topic: topicName, partition: inputs.partition, key: key, value: value, headers: built,
            timestamp: inputs.useTimestamp ? inputs.timestamp : nil)
        for issue in ProduceValidator.issues(for: request) {
            switch issue {
            case .emptyTopic, .negativePartition:
                issues.append(ProduceFormIssue(field: .partition, message: issue.message))
            case .emptyHeaderKey(let index), .headerKeyHasNul(let index):
                let row = rows.indices.contains(index) ? rows[index] : index
                let text = issue == .emptyHeaderKey(index: index)
                    ? String(localized: "Header \(row + 1) needs a name.") : String(localized: "Header \(row + 1) has a name that can't be sent.")
                issues.append(ProduceFormIssue(field: .headers, message: text))
            case .timestampBeforeEpoch:
                issues.append(ProduceFormIssue(field: .timestamp, message: issue.message))
            case .tooLarge:
                issues.append(ProduceFormIssue(field: .value, message: issue.message))
            }
        }
        if let chosen = inputs.partition, !partitionIDs.isEmpty, !partitionIDs.contains(chosen) {
            issues.append(ProduceFormIssue(field: .partition, message: String(localized: "Partition \(chosen) doesn't exist.")))
        }
        let size = failed ? nil : ProduceValidator.size(of: request)
        return Parsed(request: failed ? nil : request, issues: issues, size: size)
    }

    /// The message the form describes; nil while an encoding problem makes it unsendable.
    public var request: ProduceRequest? { parsed.request }
    public var issues: [ProduceFormIssue] { parsed.issues }
    public func issues(for field: ProduceFormIssue.Field) -> [ProduceFormIssue] { parsed.issues.filter { $0.field == field } }

    /// "12 KB of 1 MB", or nil while the bytes can't be worked out.
    public var sizeText: String? {
        parsed.size.map {
            String(localized: "\(ProduceValidator.sizeText($0)) of \(ProduceValidator.sizeText(ProduceValidator.defaultMaxMessageBytes))")
        }
    }

    public var isOverSizeLimit: Bool { (parsed.size ?? 0) > ProduceValidator.defaultMaxMessageBytes }

    // MARK: Editing

    public func addHeader() { headers.append(HeaderDraft()) }

    public func removeHeader(_ id: UUID) { headers.removeAll { $0.id == id } }

    public var canFormatValue: Bool {
        if case .success = ProduceEncoder.formatJSON(valueText), valueEncoding == .json, !isTombstone { return true }
        return false
    }

    /// Re-indents the JSON value. Does nothing for anything that is not valid JSON.
    public func formatValue() {
        guard valueEncoding == .json, !isTombstone, case .success(let text) = ProduceEncoder.formatJSON(valueText) else { return }
        valueText = text
    }

    // MARK: Sending

    public var writeGate: WriteGate { WriteGate(isReadOnly: session.isReadOnly) }

    public var isSending: Bool { state == .sending }

    /// Why Send is not offered; nil when it is.
    public var sendDisabledReason: String? {
        if let reason = writeGate.disabledReason { return reason }
        if isSending { return String(localized: "Sending…") }
        if let issue = parsed.issues.first { return issue.message }
        if parsed.request == nil { return String(localized: "Fix the problems above first") }
        return nil
    }

    public var canSend: Bool { sendDisabledReason == nil }

    /// A production cluster asks for the topic name first; anywhere else this sends at once.
    public func requestSend() {
        guard canSend else { return }
        guard session.connection.environment == .production else {
            confirmSend()
            return
        }
        let times = repeatCount
        let what = times == 1 ? String(localized: "One message is") : String(localized: "\(times) messages are")
        pendingConfirmation = DestructiveConfirmation(
            title: String(localized: "Send to production?"),
            clusterName: session.connection.name,
            environment: session.connection.environment,
            objectKind: String(localized: "topic"),
            objectName: topicName,
            actionTitle: String(localized: "Send"),
            consequences: String(localized: "\(what) added to the topic \(topicName) on \(session.connection.name). Consumers will read it, and it can't be taken back."))
    }

    /// Owned by this model, so closing a nested sheet does not cancel a send that is already on its way.
    public func confirmSend() {
        guard sendTask == nil, canSend else { return }
        sendTask = Task { [weak self] in
            await self?.send()
            self?.sendTask = nil
        }
    }

    /// Ends a repeat after the message in flight.
    public func stop() { sendTask?.cancel() }

    public func send() async {
        guard !isSending, canSend, let request = parsed.request else { return }
        let total = repeatCount
        state = .sending
        progress = (0, total)
        var done = 0
        var last: ProduceResult?
        do {
            let client = try await session.ensureConnected()
            while done < total {
                try Task.checkCancellation()
                last = try await client.produce(request)
                done += 1
                progress = (done, total)
            }
        } catch {
            progress = nil
            if error is CancellationError, let last {
                state = .sent(ProduceSummary(sent: done, requested: total, last: last))
                onSent(last)
                return
            }
            if error is CancellationError {
                state = .idle
                return
            }
            let reason = Self.message(for: error)
            state = .failed(message: done > 0 ? String(localized: "Sent \(done) of \(total), then it failed. \(reason)") : reason)
            if let last { onSent(last) }
            return
        }
        progress = nil
        if let last {
            state = .sent(ProduceSummary(sent: done, requested: total, last: last))
            onSent(last)
        }
    }

    /// Back to editing, with every field as it was.
    public func sendAnother() {
        guard !isSending else { return }
        state = .idle
    }

    public func dismissFailure() {
        if case .failed = state { state = .idle }
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case let error as KafkaError: error.userMessage
        case let error as ClusterSessionError: error.userMessage
        default: String(localized: "Couldn't send the message.")
        }
    }
}
