import Foundation
import KafkaManagerCore

/// Something wrong with a partition. Shown with an icon and text, never color alone.
public enum PartitionFlag: Sendable, Hashable, CaseIterable {
    case underReplicated, noLeader, invalidOffsets

    public var label: String {
        switch self {
        case .underReplicated: String(localized: "Under-replicated")
        case .noLeader: String(localized: "No leader")
        case .invalidOffsets: String(localized: "Invalid offsets")
        }
    }

    public var symbolName: String {
        switch self {
        case .underReplicated: "exclamationmark.triangle.fill"
        case .noLeader: "xmark.octagon.fill"
        case .invalidOffsets: "questionmark.diamond.fill"
        }
    }
}

public struct PartitionRow: Identifiable, Sendable, Equatable {
    public let id: Int
    public let leaderText: String
    public let replicasText: String
    public let isrText: String
    /// Replicas that are not in the ISR, in replica order.
    public let outOfSync: [Int]
    public let startOffset: Int64
    public let endOffset: Int64
    /// end − start, never negative.
    public let messageCount: Int64
    public let flags: [PartitionFlag]

    public var isHealthy: Bool { flags.isEmpty }
    /// "out of sync: 3"; nil when every replica is in the ISR.
    public var outOfSyncText: String? {
        outOfSync.isEmpty ? nil : String(localized: "out of sync: \(outOfSync.map(String.init).joined(separator: ", "))")
    }
    public var flagsText: String { flags.map(\.label).formatted(.list(type: .and, width: .narrow)) }
}

public enum PartitionPresentation {
    public static func rows(_ partitions: [PartitionInfo]) -> [PartitionRow] {
        partitions.sorted { $0.id < $1.id }.map(row)
    }

    public static func row(_ partition: PartitionInfo) -> PartitionRow {
        var flags: [PartitionFlag] = []
        if partition.isUnderReplicated { flags.append(.underReplicated) }
        if partition.isLeaderless { flags.append(.noLeader) }
        if partition.hasInvalidOffsets { flags.append(.invalidOffsets) }
        return PartitionRow(
            id: partition.id,
            leaderText: partition.isLeaderless ? "—" : partition.leader.map(String.init) ?? "—",
            replicasText: list(partition.replicas),
            isrText: list(partition.isr),
            outOfSync: partition.replicas.filter { !partition.isr.contains($0) },
            startOffset: partition.startOffset,
            endOffset: partition.endOffset,
            messageCount: partition.messageCount,
            flags: flags)
    }

    /// "2 under-replicated, 1 without a leader"; nil when every partition is fine.
    public static func summary(_ rows: [PartitionRow]) -> String? {
        let under = rows.filter { $0.flags.contains(.underReplicated) }.count
        let leaderless = rows.filter { $0.flags.contains(.noLeader) }.count
        var parts: [String] = []
        if under > 0 { parts.append(String(localized: "\(under) under-replicated")) }
        if leaderless > 0 { parts.append(String(localized: "\(leaderless) without a leader")) }
        return parts.isEmpty ? nil : parts.joined(separator: ", ")
    }

    private static func list(_ brokers: [Int]) -> String {
        brokers.isEmpty ? "—" : brokers.map(String.init).joined(separator: ", ")
    }
}

public struct ConfigRow: Identifiable, Sendable, Equatable {
    public var id: String { name }
    public let name: String
    /// "Hidden" for a sensitive entry, "—" when the broker returned no value.
    public let valueText: String
    public let isNonDefault: Bool
    public let isSensitive: Bool
}

public enum ConfigPresentation {
    public static let hiddenValue = String(localized: "Hidden")

    /// Settings that differ from the default come first, then by name.
    public static func rows(_ entries: [ConfigEntry], onlyNonDefault: Bool = false) -> [ConfigRow] {
        entries
            .filter { !onlyNonDefault || !$0.isDefault }
            .sorted { lhs, rhs in lhs.isDefault != rhs.isDefault ? !lhs.isDefault : lhs.name < rhs.name }
            .map { entry in
                ConfigRow(
                    name: entry.name,
                    valueText: entry.isSensitive ? hiddenValue : (entry.value ?? "—"),
                    isNonDefault: !entry.isDefault,
                    isSensitive: entry.isSensitive)
            }
    }
}
