import Foundation

public struct GroupSummary: Identifiable, Hashable, Sendable {
    public let id: String
    public let state: String
    public let memberCount: Int

    public init(id: String, state: String, memberCount: Int) {
        self.id = id
        self.state = state
        self.memberCount = memberCount
    }
}

public struct GroupMemberInfo: Identifiable, Hashable, Sendable {
    public let id: String
    public let clientId: String
    public let host: String
    /// Assigned partitions keyed by topic name.
    public let assignments: [String: [Int]]

    public init(id: String, clientId: String, host: String, assignments: [String: [Int]] = [:]) {
        self.id = id
        self.clientId = clientId
        self.host = host
        self.assignments = assignments
    }
}

public struct PartitionLag: Hashable, Sendable {
    public let topic: String
    public let partition: Int
    /// nil when the group has never committed an offset for this partition.
    public let committedOffset: Int64?
    public let endOffset: Int64

    public init(topic: String, partition: Int, committedOffset: Int64?, endOffset: Int64) {
        self.topic = topic
        self.partition = partition
        self.committedOffset = committedOffset
        self.endOffset = endOffset
    }

    /// nil when there is no committed offset ("no offset"); never negative otherwise.
    public var lag: Int64? {
        committedOffset.map { max(0, endOffset - $0) }
    }
}

public struct GroupDetail: Identifiable, Hashable, Sendable {
    public let id: String
    public let state: String
    public let members: [GroupMemberInfo]
    public let lags: [PartitionLag]

    public init(id: String, state: String, members: [GroupMemberInfo], lags: [PartitionLag]) {
        self.id = id
        self.state = state
        self.members = members
        self.lags = lags
    }
}
