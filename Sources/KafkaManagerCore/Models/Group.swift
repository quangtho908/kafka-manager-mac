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
    public let startOffset: Int64
    public let endOffset: Int64

    public init(topic: String, partition: Int, committedOffset: Int64?, startOffset: Int64, endOffset: Int64) {
        self.topic = topic
        self.partition = partition
        self.committedOffset = committedOffset
        self.startOffset = startOffset
        self.endOffset = endOffset
    }

    public var hasCommittedOffset: Bool { committedOffset != nil }

    /// Messages not yet consumed; without a commit the whole retained range counts. Never negative.
    public var lag: Int64 {
        max(0, endOffset - (committedOffset ?? startOffset))
    }

    /// True when the group committed past the log end (e.g. after topic recreation or truncation).
    public var isCommittedAheadOfEnd: Bool {
        committedOffset.map { $0 > endOffset } ?? false
    }
}

public struct GroupDetail: Identifiable, Hashable, Sendable {
    public let id: String
    public let state: String
    public let members: [GroupMemberInfo]
    public let lags: [PartitionLag]
    /// Broker id of the group coordinator; nil when the client doesn't report it.
    public let coordinatorId: Int?
    public let coordinatorHost: String?
    /// Partition assignment strategy ("range", "cooperative-sticky", …); nil for an empty group or when unknown.
    public let assignor: String?

    public init(
        id: String, state: String, members: [GroupMemberInfo], lags: [PartitionLag],
        coordinatorId: Int? = nil, coordinatorHost: String? = nil, assignor: String? = nil
    ) {
        self.id = id
        self.state = state
        self.members = members
        self.lags = lags
        self.coordinatorId = coordinatorId
        self.coordinatorHost = coordinatorHost
        self.assignor = assignor
    }
}
