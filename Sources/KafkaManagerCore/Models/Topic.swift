import Foundation

public struct TopicSummary: Identifiable, Hashable, Sendable {
    public var id: String { name }
    public let name: String
    public let partitionCount: Int
    public let replicationFactor: Int

    public init(name: String, partitionCount: Int, replicationFactor: Int) {
        self.name = name
        self.partitionCount = partitionCount
        self.replicationFactor = replicationFactor
    }

    /// Internal topics (`__consumer_offsets`, `__transaction_state`, `_schemas`, `_confluent-*`) are the ones whose
    /// name starts with an underscore. A user topic that starts with "_" is hidden by this rule too; "Show internal"
    /// brings it back.
    public var isInternal: Bool { Self.isInternalName(name) }

    public static func isInternalName(_ name: String) -> Bool { name.hasPrefix("_") }
}

/// One topic config setting. A sensitive entry never holds its value: the initializer drops it.
public struct ConfigEntry: Identifiable, Hashable, Sendable {
    public var id: String { name }
    public let name: String
    /// nil when the value is sensitive or the broker did not return one.
    public let value: String?
    /// True when the broker's default is in effect; false when the topic overrides it.
    public let isDefault: Bool
    public let isSensitive: Bool

    public init(name: String, value: String?, isDefault: Bool, isSensitive: Bool = false) {
        self.name = name
        self.value = isSensitive ? nil : value
        self.isDefault = isDefault
        self.isSensitive = isSensitive
    }
}

public struct TopicDetail: Identifiable, Hashable, Sendable {
    public var id: String { name }
    public let name: String
    public let partitions: [PartitionInfo]
    /// Non-sensitive config values by name.
    public let config: [String: String]
    /// Every setting with its default / sensitive flags, sorted by name.
    public let configEntries: [ConfigEntry]

    /// - Parameter configEntries: when nil, `config` is taken as the topic's explicit overrides (so none is a default).
    public init(name: String, partitions: [PartitionInfo], config: [String: String] = [:], configEntries: [ConfigEntry]? = nil) {
        self.name = name
        self.partitions = partitions
        let entries = configEntries ?? config.map { ConfigEntry(name: $0.key, value: $0.value, isDefault: false) }
        self.configEntries = entries.sorted { $0.name < $1.name }
        self.config = configEntries == nil ? config : Dictionary(
            entries.compactMap { entry in entry.value.map { (entry.name, $0) } }, uniquingKeysWith: { first, _ in first })
    }
}

public struct PartitionInfo: Identifiable, Hashable, Sendable {
    public let id: Int
    /// Broker id of the leader; nil while the partition has no leader.
    public let leader: Int?
    public let replicas: [Int]
    public let isr: [Int]
    public let startOffset: Int64
    public let endOffset: Int64

    public init(id: Int, leader: Int?, replicas: [Int], isr: [Int], startOffset: Int64, endOffset: Int64) {
        self.id = id
        self.leader = leader
        self.replicas = replicas
        self.isr = isr
        self.startOffset = startOffset
        self.endOffset = endOffset
    }

    /// Never negative; see `hasInvalidOffsets`.
    public var messageCount: Int64 { max(0, endOffset - startOffset) }
    /// The end offset is before the start offset, which a healthy partition never reports.
    public var hasInvalidOffsets: Bool { endOffset < startOffset }
    public var isLeaderless: Bool { leader == nil || leader == -1 }
    public var isUnderReplicated: Bool { isr.count < replicas.count }
}

public struct NewTopic: Hashable, Sendable {
    public let name: String
    public let partitions: Int
    public let replicationFactor: Int
    public let config: [String: String]

    public init(name: String, partitions: Int, replicationFactor: Int, config: [String: String] = [:]) {
        self.name = name
        self.partitions = partitions
        self.replicationFactor = replicationFactor
        self.config = config
    }
}
