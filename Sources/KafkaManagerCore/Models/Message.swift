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

public struct KafkaMessage: Hashable, Sendable {
    public let partition: Int
    public let offset: Int64
    public let timestamp: Date
    public let key: Data?
    public let value: Data?
    public let headers: [MessageHeader]

    public init(
        partition: Int,
        offset: Int64,
        timestamp: Date,
        key: Data?,
        value: Data?,
        headers: [MessageHeader] = []
    ) {
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp
        self.key = key
        self.value = value
        self.headers = headers
    }
}
