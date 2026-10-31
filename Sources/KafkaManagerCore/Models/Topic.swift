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
}

public struct TopicDetail: Identifiable, Hashable, Sendable {
    public var id: String { name }
    public let name: String
    public let partitions: [PartitionInfo]
    public let config: [String: String]

    public init(name: String, partitions: [PartitionInfo], config: [String: String] = [:]) {
        self.name = name
        self.partitions = partitions
        self.config = config
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

    public var messageCount: Int64 { max(0, endOffset - startOffset) }
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
