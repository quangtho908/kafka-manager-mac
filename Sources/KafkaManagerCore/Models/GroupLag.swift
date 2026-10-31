import Foundation

/// How worrying a lag is. The numbers behind the levels are `LagThresholds`.
public enum LagSeverity: Int, Hashable, Sendable, Comparable, CaseIterable {
    case none, low, medium, high

    public static func < (lhs: LagSeverity, rhs: LagSeverity) -> Bool { lhs.rawValue < rhs.rawValue }
}

/// Where lag turns medium and high. Defaults: any lag is at least low; more than 1 000 messages is medium; more than
/// 10 000 is high (spec 04: red above 10 000). A later setting can change them.
public struct LagThresholds: Hashable, Sendable {
    public static let standard = LagThresholds(medium: 1_000, high: 10_000)

    /// Lag above this is `.medium`.
    public let medium: Int64
    /// Lag above this is `.high`. Kept at least `medium`.
    public let high: Int64

    public init(medium: Int64, high: Int64) {
        self.medium = max(medium, 0)
        self.high = max(high, self.medium)
    }

    /// 0 → none, 1…medium → low, medium+1…high → medium, above high → high. A negative lag counts as 0.
    public func severity(of lag: Int64) -> LagSeverity {
        if lag <= 0 { return .none }
        if lag > high { return .high }
        if lag > medium { return .medium }
        return .low
    }
}

/// One topic's partitions of a group, with the totals the lag tab shows.
public struct TopicLag: Identifiable, Hashable, Sendable {
    public let topic: String
    /// Ordered by partition.
    public let partitions: [PartitionLag]
    public let totalLag: Int64
    /// Some partition has never been committed (its lag counts the whole retained range).
    public let hasUncommittedPartitions: Bool
    /// Some partition is committed past the log end (its lag was clamped to 0).
    public let hasCommittedAheadOfEnd: Bool

    public var id: String { topic }
    public var hasWarning: Bool { hasCommittedAheadOfEnd }
}

public enum GroupLag {
    /// Sum of the partitions' lag, never negative, saturating instead of overflowing.
    public static func total(_ lags: [PartitionLag]) -> Int64 {
        lags.reduce(into: Int64(0)) { sum, lag in
            let (result, overflow) = sum.addingReportingOverflow(lag.lag)
            sum = overflow ? Int64.max : result
        }
    }

    /// Partitions grouped by topic, topics ordered by name and partitions by number, each with its total.
    public static func byTopic(_ lags: [PartitionLag]) -> [TopicLag] {
        Dictionary(grouping: lags, by: \.topic)
            .map { topic, partitions in
                let ordered = partitions.sorted { $0.partition < $1.partition }
                return TopicLag(
                    topic: topic, partitions: ordered, totalLag: total(ordered),
                    hasUncommittedPartitions: ordered.contains { !$0.hasCommittedOffset },
                    hasCommittedAheadOfEnd: ordered.contains { $0.isCommittedAheadOfEnd })
            }
            .sorted { $0.topic < $1.topic }
    }

    /// The worst severity among the partitions, so one bad partition isn't hidden by a small total.
    public static func severity(of lags: [PartitionLag], thresholds: LagThresholds = .standard) -> LagSeverity {
        lags.map { thresholds.severity(of: $0.lag) }.max() ?? .none
    }

    /// "1,234" with grouping, for exact values.
    public static func exactText(_ lag: Int64) -> String {
        max(lag, 0).formatted(.number.grouping(.automatic))
    }

    /// Short form for tight columns: 999 → "999", 1 234 → "1.2K", 1 500 000 → "1.5M", 2 000 000 000 → "2B".
    public static func shortText(_ lag: Int64) -> String {
        let value = max(lag, 0)
        let units: [(Int64, String)] = [(1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")]
        for (size, suffix) in units where value >= size {
            let tenths = (value * 10) / size
            let whole = tenths / 10
            let fraction = tenths % 10
            return fraction == 0 ? "\(whole)\(suffix)" : "\(whole).\(fraction)\(suffix)"
        }
        return String(value)
    }

    /// What the table says about a partition's offset: "no offset" when the group never committed one.
    public static func commitText(_ lag: PartitionLag) -> String {
        guard let committed = lag.committedOffset else { return String(localized: "no offset") }
        return String(committed)
    }
}
