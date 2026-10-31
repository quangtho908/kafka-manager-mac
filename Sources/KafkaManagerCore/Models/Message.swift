import Foundation

public enum StartPosition: Hashable, Sendable {
    /// The most recent N messages.
    case latest(Int)
    case earliest
    case offset(Int64)
    case timestamp(Date)
}

public struct MessageQuery: Hashable, Sendable {
    public let topic: String
    /// nil reads all partitions.
    public let partition: Int?
    public let start: StartPosition
    public let limit: Int

    public init(topic: String, partition: Int? = nil, start: StartPosition, limit: Int) {
        self.topic = topic
        self.partition = partition
        self.start = start
        self.limit = limit
    }
}

public struct MessageHeader: Hashable, Sendable {
    public let key: String
    public let value: Data?

    public init(key: String, value: Data?) {
        self.key = key
        self.value = value
    }
}

/// Who set a message's timestamp.
public enum TimestampType: String, Hashable, Sendable {
    /// The producer set it.
    case createTime
    /// The broker set it when appending to the log.
    case logAppendTime
    /// The message has no timestamp (old message format); `KafkaMessage.timestamp` is the Unix epoch then.
    case notAvailable
}

public struct KafkaMessage: Hashable, Sendable {
    public let partition: Int
    public let offset: Int64
    public let timestamp: Date
    public let key: Data?
    public let value: Data?
    public let headers: [MessageHeader]
    /// nil when the source doesn't say (the fake client).
    public let timestampType: TimestampType?

    public init(
        partition: Int,
        offset: Int64,
        timestamp: Date,
        key: Data?,
        value: Data?,
        headers: [MessageHeader] = [],
        timestampType: TimestampType? = nil
    ) {
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp
        self.key = key
        self.value = value
        self.headers = headers
        self.timestampType = timestampType
    }
}
