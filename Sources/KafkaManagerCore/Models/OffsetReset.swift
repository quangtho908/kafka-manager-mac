import Foundation

/// Where a group's committed offsets should move to.
public enum OffsetResetTarget: Hashable, Sendable {
    /// The log end: everything already in the topic is skipped.
    case latest
    /// The log start: everything still retained is read again.
    case earliest
    /// The first message at or after this time; nothing at or after it means the log end.
    case timestamp(Date)
    /// This offset, clamped into the partition's retained range.
    case offset(Int64)
}

/// Which partitions of the group a reset touches.
public enum OffsetResetScope: Hashable, Sendable {
    case wholeGroup
    case topics(Set<String>)
    case partitions(topic: String, Set<Int>)

    public func includes(topic: String, partition: Int) -> Bool {
        switch self {
        case .wholeGroup: true
        case .topics(let topics): topics.contains(topic)
        case .partitions(let name, let ids): name == topic && ids.contains(partition)
        }
    }
}

/// One partition of a preview: what the group has now, what it would have, and what that does to the lag.
public struct OffsetResetPreviewRow: Identifiable, Hashable, Sendable {
    public let topic: String
    public let partition: Int
    /// nil when the group never committed this partition.
    public let currentCommitted: Int64?
    public let newOffset: Int64
    public let start: Int64
    public let end: Int64
    public let lagBefore: Int64
    public let lagAfter: Int64
    /// The requested offset was outside the retained range and was moved to its edge.
    public let isClamped: Bool
    /// False only when the group already sits exactly on `newOffset`.
    public let willChange: Bool

    public var id: String { "\(topic)#\(partition)" }

    public init(
        topic: String, partition: Int, currentCommitted: Int64?, newOffset: Int64, start: Int64, end: Int64,
        lagBefore: Int64, lagAfter: Int64, isClamped: Bool, willChange: Bool
    ) {
        self.topic = topic
        self.partition = partition
        self.currentCommitted = currentCommitted
        self.newOffset = newOffset
        self.start = start
        self.end = end
        self.lagBefore = lagBefore
        self.lagAfter = lagAfter
        self.isClamped = isClamped
        self.willChange = willChange
    }
}

public struct OffsetResetPreview: Hashable, Sendable {
    public let group: String
    /// The group's state when the preview was built; offsets can only change while it is Empty or Dead.
    public let groupState: GroupState
    public let rows: [OffsetResetPreviewRow]

    public init(group: String, groupState: GroupState, rows: [OffsetResetPreviewRow]) {
        self.group = group
        self.groupState = groupState
        self.rows = rows
    }

    public var changedRows: [OffsetResetPreviewRow] { rows.filter(\.willChange) }
    public var clampedCount: Int { rows.filter(\.isClamped).count }
    public var totalLagBefore: Int64 { rows.reduce(0) { OffsetResetPlanner.saturatingAdd($0, $1.lagBefore) } }
    public var totalLagAfter: Int64 { rows.reduce(0) { OffsetResetPlanner.saturatingAdd($0, $1.lagAfter) } }

    /// The same preview limited to the rows `scope` includes.
    public func limited(to scope: OffsetResetScope) -> OffsetResetPreview {
        OffsetResetPreview(group: group, groupState: groupState, rows: rows.filter { scope.includes(topic: $0.topic, partition: $0.partition) })
    }

    /// Only what changes is sent to the broker.
    public var plan: OffsetResetPlan {
        OffsetResetPlan(assignments: changedRows.map { OffsetResetAssignment(topic: $0.topic, partition: $0.partition, offset: $0.newOffset) })
    }
}

public struct OffsetResetAssignment: Hashable, Sendable {
    public let topic: String
    public let partition: Int
    public let offset: Int64

    public init(topic: String, partition: Int, offset: Int64) {
        self.topic = topic
        self.partition = partition
        self.offset = offset
    }
}

/// What gets committed: one new offset per partition.
public struct OffsetResetPlan: Hashable, Sendable {
    public let assignments: [OffsetResetAssignment]

    public init(assignments: [OffsetResetAssignment]) {
        self.assignments = assignments
    }

    public var isEmpty: Bool { assignments.isEmpty }
}

public struct OffsetResetFailure: Hashable, Sendable {
    public let topic: String
    public let partition: Int
    /// Plain-language reason, already safe to show.
    public let message: String

    public init(topic: String, partition: Int, message: String) {
        self.topic = topic
        self.partition = partition
        self.message = message
    }
}

/// The broker's answer, partition by partition. A reset can succeed for some partitions and fail for others.
public struct OffsetResetResult: Hashable, Sendable {
    public let applied: Int
    public let failures: [OffsetResetFailure]

    public init(applied: Int, failures: [OffsetResetFailure] = []) {
        self.applied = applied
        self.failures = failures
    }

    public var isComplete: Bool { failures.isEmpty }
}

/// Pure planning over what was fetched: no I/O here.
public enum OffsetResetPlanner {
    /// - Parameters:
    ///   - lags: the group's partitions with committed offset and the retained range (`GroupDetail.lags`).
    ///   - timestampOffsets: for `.timestamp`, the first offset at or after the time per partition; a missing entry or a
    ///     negative value means "no such message", which resolves to the log end.
    public static func rows(
        lags: [PartitionLag], target: OffsetResetTarget, scope: OffsetResetScope = .wholeGroup,
        timestampOffsets: [PartitionKey: Int64] = [:]
    ) -> [OffsetResetPreviewRow] {
        lags
            .filter { scope.includes(topic: $0.topic, partition: $0.partition) }
            .sorted { ($0.topic, $0.partition) < ($1.topic, $1.partition) }
            .map { row(for: $0, target: target, timestampOffset: timestampOffsets[PartitionKey(topic: $0.topic, partition: $0.partition)]) }
    }

    public static func preview(
        detail: GroupDetail, target: OffsetResetTarget, scope: OffsetResetScope = .wholeGroup,
        timestampOffsets: [PartitionKey: Int64] = [:]
    ) -> OffsetResetPreview {
        OffsetResetPreview(
            group: detail.id, groupState: detail.groupState,
            rows: rows(lags: detail.lags, target: target, scope: scope, timestampOffsets: timestampOffsets))
    }

    static func row(for lag: PartitionLag, target: OffsetResetTarget, timestampOffset: Int64?) -> OffsetResetPreviewRow {
        let requested: Int64
        switch target {
        case .latest: requested = lag.endOffset
        case .earliest: requested = lag.startOffset
        case .timestamp: requested = (timestampOffset.map { $0 >= 0 } ?? false) ? (timestampOffset ?? lag.endOffset) : lag.endOffset
        case .offset(let offset): requested = offset
        }
        // A range that is backwards (should not happen) is treated as empty at its start.
        let high = max(lag.endOffset, lag.startOffset)
        let new = min(max(requested, lag.startOffset), high)
        return OffsetResetPreviewRow(
            topic: lag.topic, partition: lag.partition, currentCommitted: lag.committedOffset, newOffset: new,
            start: lag.startOffset, end: lag.endOffset, lagBefore: lag.lag, lagAfter: max(0, lag.endOffset - new),
            isClamped: new != requested, willChange: lag.committedOffset != new)
    }

    public static func saturatingAdd(_ a: Int64, _ b: Int64) -> Int64 {
        let (sum, overflow) = a.addingReportingOverflow(b)
        return overflow ? Int64.max : sum
    }
}

/// A partition's address, independent of any client library.
public struct PartitionKey: Hashable, Sendable {
    public let topic: String
    public let partition: Int

    public init(topic: String, partition: Int) {
        self.topic = topic
        self.partition = partition
    }
}

/// Whether offsets may be changed for a group in this state. The broker stays the authority.
public enum OffsetResetRule {
    public static let activeMembersMessage = String(localized: "Stop the group's consumers first; offsets can only be changed while the group has no active members.")

    /// Nil when the state allows a reset.
    public static func refusal(for state: GroupState) -> String? {
        switch state {
        case .empty, .dead: nil
        case .stable, .preparingRebalance, .completingRebalance, .other: activeMembersMessage
        }
    }
}

extension KafkaError {
    /// Kafka's NON_EMPTY_GROUP (68), also used for every way the broker says "this group has members": a reset or
    /// delete was refused for that reason.
    public static let nonEmptyGroupCode = 68

    public var isNonEmptyGroup: Bool {
        if case .clusterError(let code, _) = self { return code == Self.nonEmptyGroupCode }
        return false
    }
}
