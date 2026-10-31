import Foundation
import Observation
import KafkaManagerCore

public enum MessageStartKind: Sendable, Hashable, CaseIterable {
    case latest, earliest, offset, timestamp
}

public enum MessageFinishReason: Sendable, Equatable {
    case limitReached
    case endOfData
    case stopped
    /// The rows kept in memory reached the byte budget before the row limit.
    case sizeLimit
}

public enum MessageFetchStatus: Sendable, Equatable {
    case idle
    case fetching(count: Int)
    case finished(count: Int, reason: MessageFinishReason)
    /// The rows received before the error stay in the table.
    case failed(message: String, keptCount: Int)
}

/// Query form, streaming fetch and client-side filter/sort for one topic's messages.
///
/// Nothing observable changes per message: arriving rows go to a plain buffer that is published at most every
/// `flushInterval` while fetching, and once more when the fetch ends. The fetch runs in the caller's task (the view's
/// `.task`), so closing the tab or switching topic cancels it; `stop()` cancels it too.
@MainActor @Observable
public final class MessagesViewModel {
    public static let defaultLimit = 500
    public static let maxLimit = 10_000
    public static let defaultLatestCount = 100
    public static let defaultFlushInterval: Duration = .milliseconds(100)
    /// Total key+value bytes kept in memory; reaching it ends the fetch (`.sizeLimit`).
    public static let defaultByteBudget = 64 * 1024 * 1024

    // MARK: Query form

    /// nil reads all partitions.
    public var partition: Int?
    public var startKind = MessageStartKind.latest
    public var latestCount = MessagesViewModel.defaultLatestCount
    public var startOffset: Int64 = 0
    public var startDate = Date().addingTimeInterval(-3600)
    /// What the user asked for; the query uses `limit`.
    public var limitInput = MessagesViewModel.defaultLimit

    // MARK: Results

    public private(set) var status = MessageFetchStatus.idle
    public private(set) var rows: [MessageRow] = []
    /// Remarks about the last fetch's query (offset adjusted to the partition's range).
    public private(set) var queryNotes: [String] = []
    public var searchText = ""
    public var sortKey = MessageSortKey.timestamp
    public var sortAscending = false
    public var selection: MessageID?

    @ObservationIgnored private let session: ClusterSession
    @ObservationIgnored public let topicName: String
    @ObservationIgnored private let partitionInfo: @MainActor () -> [PartitionInfo]
    @ObservationIgnored private let flushInterval: Duration
    @ObservationIgnored private let sleep: @Sendable (Duration) async throws -> Void
    @ObservationIgnored private let byteBudget: Int

    @ObservationIgnored private var buffer: [MessageRow] = []
    @ObservationIgnored private var generation = 0
    @ObservationIgnored private var inner: Task<Void, Never>?
    @ObservationIgnored private(set) var publishCount = 0
    @ObservationIgnored private var revision = 0
    @ObservationIgnored private var sortedCache: (revision: Int, key: MessageSortKey, ascending: Bool, rows: [MessageRow])?
    @ObservationIgnored private var visibleCache: (revision: Int, key: MessageSortKey, ascending: Bool, search: String, rows: [MessageRow])?

    /// - Parameter partitionInfo: the topic's partitions as last loaded, for the partition picker and offset clamping.
    public init(
        session: ClusterSession,
        topicName: String,
        partitionInfo: @escaping @MainActor () -> [PartitionInfo] = { [] },
        flushInterval: Duration = MessagesViewModel.defaultFlushInterval,
        byteBudget: Int = MessagesViewModel.defaultByteBudget,
        sleep: @escaping @Sendable (Duration) async throws -> Void = { try await Task.sleep(for: $0) }
    ) {
        self.session = session
        self.topicName = topicName
        self.partitionInfo = partitionInfo
        self.flushInterval = flushInterval
        self.byteBudget = byteBudget
        self.sleep = sleep
    }

    // MARK: Query validation

    public var partitionIDs: [Int] { partitionInfo().map(\.id).sorted() }

    /// The limit actually used: `limitInput` clamped to 1…10 000.
    public var limit: Int { min(max(limitInput, 1), Self.maxLimit) }

    /// Shown beside the limit field when the input was changed.
    public var limitNote: String? {
        if limitInput < 1 { return String(localized: "The limit is at least 1.") }
        if limitInput > Self.maxLimit { return String(localized: "The limit is at most \(Self.maxLimit).") }
        return nil
    }

    public enum QueryIssue: Hashable, Sendable {
        case latestCount, offset, partition

        public var message: String {
            switch self {
            case .latestCount: String(localized: "Enter how many recent messages to read (1 or more).")
            case .offset: String(localized: "The offset can't be negative.")
            case .partition: String(localized: "That partition doesn't exist.")
            }
        }
    }

    public var queryIssues: [QueryIssue] {
        var issues: [QueryIssue] = []
        if startKind == .latest, latestCount < 1 { issues.append(.latestCount) }
        if startKind == .offset, startOffset < 0 { issues.append(.offset) }
        if let partition, !partitionInfo().isEmpty, !partitionInfo().contains(where: { $0.id == partition }) { issues.append(.partition) }
        return issues
    }

    public var isFetching: Bool {
        if case .fetching = status { true } else { false }
    }

    public var canFetch: Bool { queryIssues.isEmpty && !isFetching }

    /// The query to send and notes about what was adjusted. An offset outside the partition's current range is
    /// clamped to it when one partition is chosen; with All partitions the offset applies to each, unadjusted.
    func resolvedQuery() -> (query: MessageQuery, notes: [String]) {
        var notes: [String] = []
        let start: StartPosition
        switch startKind {
        case .latest: start = .latest(max(latestCount, 1))
        case .earliest: start = .earliest
        case .timestamp: start = .timestamp(startDate)
        case .offset:
            var offset = max(startOffset, 0)
            if let partition, let info = partitionInfo().first(where: { $0.id == partition }) {
                if offset < info.startOffset {
                    notes.append(String(localized: "Offset \(offset) is before the first available offset; starting at \(info.startOffset)."))
                    offset = info.startOffset
                } else if offset > info.endOffset {
                    notes.append(String(localized: "Offset \(offset) is past the end; starting at \(info.endOffset)."))
                    offset = info.endOffset
                }
            }
            start = .offset(offset)
        }
        if let message = limitNote { notes.append(message) }
        return (MessageQuery(topic: topicName, partition: partition, start: start, limit: limit), notes)
    }

    // MARK: Fetching

    /// Runs one fetch to its end. Returns when it finishes, fails, hits the limit, or is stopped (by `stop()` or by
    /// cancelling the calling task). A new call replaces a fetch still running.
    public func fetch() async {
        guard queryIssues.isEmpty else { return }
        inner?.cancel()
        generation += 1
        let id = generation
        let (query, notes) = resolvedQuery()
        queryNotes = notes
        rows = []
        buffer = []
        selection = nil
        revision += 1
        status = .fetching(count: 0)

        let task = Task { await self.run(query, id: id) }
        inner = task
        await withTaskCancellationHandler { await task.value } onCancel: { task.cancel() }
    }

    /// Ends a running fetch; the rows received so far stay.
    public func stop() {
        inner?.cancel()
    }

    private func run(_ query: MessageQuery, id: Int) async {
        let flusher = Task { [weak self, sleep, flushInterval] in
            while !Task.isCancelled {
                do { try await sleep(flushInterval) } catch { return }
                guard let self, !Task.isCancelled else { return }
                self.flush(id: id)
            }
        }
        let outcome = await consume(query, id: id)
        flusher.cancel()
        guard id == generation else { return }
        flush(id: id)
        switch outcome {
        case .finished(let reason): status = .finished(count: rows.count, reason: reason)
        case .failed(let message): status = .failed(message: message, keptCount: rows.count)
        }
        if id == generation { inner = nil }
    }

    private enum Outcome {
        case finished(MessageFinishReason)
        case failed(String)
    }

    private func consume(_ query: MessageQuery, id: Int) async -> Outcome {
        var received = 0
        var bytes = 0
        do {
            let client = try await session.ensureConnected()
            for try await message in client.fetchMessages(query) {
                guard id == generation else { return .finished(.stopped) }
                if Task.isCancelled { return .finished(.stopped) }
                let row = MessageRow(message)
                buffer.append(row)
                received += 1
                bytes += row.size
                if received >= query.limit { return .finished(.limitReached) }
                if bytes >= byteBudget { return .finished(.sizeLimit) }
            }
            return Task.isCancelled ? .finished(.stopped) : .finished(.endOfData)
        } catch is CancellationError {
            return .finished(.stopped)
        } catch {
            if Task.isCancelled { return .finished(.stopped) }
            return .failed(Self.message(for: error))
        }
    }

    /// Moves buffered rows into the published list. Does nothing for a fetch that was replaced.
    private func flush(id: Int) {
        guard id == generation, !buffer.isEmpty else { return }
        rows.append(contentsOf: buffer)
        buffer.removeAll(keepingCapacity: true)
        revision += 1
        publishCount += 1
        if case .fetching = status { status = .fetching(count: rows.count) }
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case let error as KafkaError: error.userMessage
        case let error as ClusterSessionError: error.userMessage
        default: String(localized: "Couldn't read the messages.")
        }
    }

    // MARK: Derived list

    /// Sorted, then filtered. Sorting reruns only when the rows or the sort change; typing runs one filter pass.
    public var visibleRows: [MessageRow] {
        let search = searchText.trimmingCharacters(in: .whitespaces)
        if let cache = visibleCache, cache.revision == revision, cache.key == sortKey, cache.ascending == sortAscending, cache.search == search {
            return cache.rows
        }
        let sorted: [MessageRow]
        if let cache = sortedCache, cache.revision == revision, cache.key == sortKey, cache.ascending == sortAscending {
            sorted = cache.rows
        } else {
            sorted = MessageRows.sort(rows, by: sortKey, ascending: sortAscending)
            sortedCache = (revision, sortKey, sortAscending, sorted)
        }
        let filtered = MessageRows.filter(sorted, search: search)
        visibleCache = (revision, sortKey, sortAscending, search, filtered)
        return filtered
    }

    public var hasNoMatches: Bool { !rows.isEmpty && visibleRows.isEmpty }

    /// "Fetched 500 messages · limit reached", or the live count while fetching.
    public var statusText: String {
        switch status {
        case .idle:
            return ""
        case .fetching(let count):
            return String(localized: "Fetching… \(count) messages")
        case .finished(let count, let reason):
            let counted = count == 1 ? String(localized: "Fetched 1 message") : String(localized: "Fetched \(count) messages")
            switch reason {
            case .limitReached: return "\(counted) · \(String(localized: "limit reached"))"
            case .endOfData: return "\(counted) · \(String(localized: "end of data"))"
            case .stopped: return "\(counted) · \(String(localized: "stopped"))"
            case .sizeLimit: return "\(counted) · \(String(localized: "size limit reached"))"
            }
        case .failed(_, let kept):
            return kept == 1 ? String(localized: "Kept 1 message") : String(localized: "Kept \(kept) messages")
        }
    }
}
