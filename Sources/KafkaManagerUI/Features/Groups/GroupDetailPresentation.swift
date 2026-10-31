import Foundation
import KafkaManagerCore

/// Symbol, text and tone for a lag level. The text and symbol carry the meaning; the tone only reinforces it.
public struct LagSeverityPresentation: Sendable, Equatable {
    public enum Tone: Sendable, Equatable { case good, caution, warning, critical }

    public let label: String
    public let symbolName: String
    public let tone: Tone

    public init(_ severity: LagSeverity) {
        switch severity {
        case .none: self.init(label: String(localized: "No lag"), symbolName: "checkmark.circle", tone: .good)
        case .low: self.init(label: String(localized: "Low"), symbolName: "exclamationmark.circle", tone: .caution)
        case .medium: self.init(label: String(localized: "Medium"), symbolName: "exclamationmark.triangle", tone: .warning)
        case .high: self.init(label: String(localized: "High"), symbolName: "exclamationmark.octagon", tone: .critical)
        }
    }

    private init(label: String, symbolName: String, tone: Tone) {
        self.label = label
        self.symbolName = symbolName
        self.tone = tone
    }
}

/// One line of the lag table: a topic (with its total) or one of its partitions.
public struct LagRow: Identifiable, Sendable, Equatable {
    public enum Kind: Sendable, Equatable { case topic, partition }

    public let id: String
    public let kind: Kind
    public let topic: String
    public let partition: Int?
    /// "no offset" when the group never committed this partition; empty for a topic line.
    public let committedText: String
    public let endOffset: Int64?
    public let lag: Int64
    public let severity: LagSeverity
    /// Client id of the member holding the partition, "—" if none; empty for a topic line.
    public let memberText: String
    /// The member id behind `memberText`, for a tooltip.
    public let memberHelp: String?
    /// Explains a flag on this line ("no offset", "committed past the end"), nil when there is nothing to say.
    public let note: String?
    public let hasNoOffset: Bool
    public let isAheadOfEnd: Bool
}

public struct LagSection: Identifiable, Sendable, Equatable {
    public let topic: String
    public let header: LagRow
    public let partitions: [LagRow]
    public var id: String { topic }
}

public struct MemberRow: Identifiable, Sendable, Equatable {
    public let id: String
    public let clientId: String
    public let host: String
    public let partitionCount: Int
    /// "orders: 0, 1, 2; payments: 0", cut with "…" after `maxPartitionsTextLength` characters.
    public let partitionsText: String
    /// The same list, whole, for a tooltip.
    public let partitionsFull: String
}

public struct GroupHeaderInfo: Sendable, Equatable {
    public let groupId: String
    public let state: GroupState
    public let coordinatorText: String
    public let assignorText: String
    public let memberCount: Int
    public let totalLag: Int64
    public let totalLagShort: String
    public let totalLagExact: String
    public let severity: LagSeverity
    public let uncommittedPartitions: Int
    public let aheadOfEndPartitions: Int

    public var warningCount: Int { uncommittedPartitions + aheadOfEndPartitions }

    /// "2 partitions without a committed offset, 1 committed past the end", or nil.
    public var warningText: String? {
        var parts: [String] = []
        if uncommittedPartitions > 0 {
            parts.append(uncommittedPartitions == 1
                ? String(localized: "1 partition without a committed offset")
                : String(localized: "\(uncommittedPartitions) partitions without a committed offset"))
        }
        if aheadOfEndPartitions > 0 {
            parts.append(aheadOfEndPartitions == 1
                ? String(localized: "1 partition committed past the end")
                : String(localized: "\(aheadOfEndPartitions) partitions committed past the end"))
        }
        return parts.isEmpty ? nil : parts.joined(separator: ", ")
    }
}

/// Pure shaping of a `GroupDetail` for the screen.
public enum GroupDetailPresentation {
    public static let maxPartitionsTextLength = 60
    private static let noMember = "—"

    public static func header(_ detail: GroupDetail, thresholds: LagThresholds = .standard) -> GroupHeaderInfo {
        let total = GroupLag.total(detail.lags)
        let coordinator: String
        switch (detail.coordinatorId, detail.coordinatorHost) {
        case (let id?, let host?): coordinator = String(localized: "broker \(id) (\(host))")
        case (let id?, nil): coordinator = String(localized: "broker \(id)")
        case (nil, let host?): coordinator = host
        case (nil, nil): coordinator = "—"
        }
        return GroupHeaderInfo(
            groupId: detail.id, state: detail.groupState, coordinatorText: coordinator,
            assignorText: detail.assignor.flatMap { $0.isEmpty ? nil : $0 } ?? "—",
            memberCount: detail.members.count, totalLag: total,
            totalLagShort: GroupLag.shortText(total), totalLagExact: GroupLag.exactText(total),
            severity: GroupLag.severity(of: detail.lags, thresholds: thresholds),
            uncommittedPartitions: detail.lags.filter { !$0.hasCommittedOffset }.count,
            aheadOfEndPartitions: detail.lags.filter(\.isCommittedAheadOfEnd).count)
    }

    /// Lag lines grouped by topic. Each topic line carries the topic total and the worst partition's severity.
    public static func lagSections(_ detail: GroupDetail, thresholds: LagThresholds = .standard) -> [LagSection] {
        var holders: [String: GroupMemberInfo] = [:]
        for member in detail.members {
            for (topic, partitions) in member.assignments {
                for partition in partitions { holders["\(topic)\u{0}\(partition)"] = member }
            }
        }
        return GroupLag.byTopic(detail.lags).map { topic in
            let rows = topic.partitions.map { lag -> LagRow in
                let holder = holders["\(lag.topic)\u{0}\(lag.partition)"]
                return LagRow(
                    id: "\(lag.topic)/\(lag.partition)", kind: .partition, topic: lag.topic, partition: lag.partition,
                    committedText: GroupLag.commitText(lag), endOffset: lag.endOffset, lag: lag.lag,
                    severity: thresholds.severity(of: lag.lag),
                    memberText: holder?.clientId ?? noMember, memberHelp: holder?.id,
                    note: note(for: lag), hasNoOffset: !lag.hasCommittedOffset, isAheadOfEnd: lag.isCommittedAheadOfEnd)
            }
            let header = LagRow(
                id: topic.topic, kind: .topic, topic: topic.topic, partition: nil, committedText: "", endOffset: nil,
                lag: topic.totalLag, severity: GroupLag.severity(of: topic.partitions, thresholds: thresholds),
                memberText: "", memberHelp: nil, note: nil,
                hasNoOffset: topic.hasUncommittedPartitions, isAheadOfEnd: topic.hasCommittedAheadOfEnd)
            return LagSection(topic: topic.topic, header: header, partitions: rows)
        }
    }

    private static func note(for lag: PartitionLag) -> String? {
        if lag.isCommittedAheadOfEnd {
            return String(localized: "Committed offset \(lag.committedOffset ?? 0) is past the end (\(lag.endOffset)); the topic may have been recreated. Lag shows 0.")
        }
        if !lag.hasCommittedOffset {
            return String(localized: "This group never committed an offset here, so lag counts everything still in the log (end − start).")
        }
        return nil
    }

    /// Members sorted by client id (then member id), with their partitions counted and listed.
    public static func memberRows(_ detail: GroupDetail) -> [MemberRow] {
        detail.members
            .sorted { ($0.clientId, $0.id) < ($1.clientId, $1.id) }
            .map { member in
                let topics = member.assignments.keys.sorted()
                let full = topics.compactMap { topic -> String? in
                    let partitions = (member.assignments[topic] ?? []).sorted()
                    return partitions.isEmpty ? nil : "\(topic): \(partitions.map(String.init).joined(separator: ", "))"
                }.joined(separator: "; ")
                let shown = full.count > maxPartitionsTextLength ? String(full.prefix(maxPartitionsTextLength)) + "…" : full
                return MemberRow(
                    id: member.id, clientId: member.clientId, host: member.host,
                    partitionCount: member.assignments.values.reduce(0) { $0 + $1.count },
                    partitionsText: full.isEmpty ? "—" : shown, partitionsFull: full.isEmpty ? "—" : full)
            }
    }
}
