import Foundation

/// One message to send. `value == nil` is a tombstone (a null value), which is not the same as an empty value.
public struct ProduceRequest: Hashable, Sendable {
    public let topic: String
    /// nil lets the default partitioner choose (by key, or spread across partitions without one).
    public let partition: Int?
    public let key: Data?
    public let value: Data?
    public let headers: [MessageHeader]
    /// nil stamps the message with the current time.
    public let timestamp: Date?

    public init(
        topic: String, partition: Int? = nil, key: Data? = nil, value: Data? = nil,
        headers: [MessageHeader] = [], timestamp: Date? = nil
    ) {
        self.topic = topic
        self.partition = partition
        self.key = key
        self.value = value
        self.headers = headers
        self.timestamp = timestamp
    }

    /// A copy of `message` to send again: same key, value and headers; the partition is chosen by the partitioner unless
    /// given, and the timestamp is now.
    public init(copying message: KafkaMessage, to topic: String, partition: Int? = nil) {
        self.init(topic: topic, partition: partition, key: message.key, value: message.value, headers: message.headers)
    }
}

/// Where the broker stored a sent message.
public struct ProduceResult: Hashable, Sendable {
    public let partition: Int
    public let offset: Int64
    public let timestamp: Date

    public init(partition: Int, offset: Int64, timestamp: Date) {
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp
    }
}

/// Why a request cannot be sent. Found before anything leaves the app.
public enum ProduceIssue: Hashable, Sendable {
    case emptyTopic
    case negativePartition
    case emptyHeaderKey(index: Int)
    /// A header name travels as a C string, so it cannot hold a NUL character.
    case headerKeyHasNul(index: Int)
    case timestampBeforeEpoch
    case tooLarge(size: Int, limit: Int)

    public var message: String {
        switch self {
        case .emptyTopic: String(localized: "Choose a topic.")
        case .negativePartition: String(localized: "The partition must be 0 or more.")
        case .emptyHeaderKey(let index): String(localized: "Header \(index + 1) needs a name.")
        case .headerKeyHasNul(let index): String(localized: "Header \(index + 1) has a name that can't be sent.")
        case .timestampBeforeEpoch: String(localized: "The timestamp must be on or after 1 January 1970.")
        case .tooLarge(let size, let limit):
            String(localized: "The message is \(ProduceValidator.sizeText(size)); the limit is \(ProduceValidator.sizeText(limit)).")
        }
    }
}

public enum ProduceValidator {
    /// 1 MiB: what a default broker accepts (`message.max.bytes` is 1 MiB plus a little batch overhead).
    public static let defaultMaxMessageBytes = 1_048_576

    /// Bytes the payload occupies: key, value, and every header's name and value.
    public static func size(of request: ProduceRequest) -> Int {
        size(key: request.key, value: request.value, headers: request.headers)
    }

    public static func size(key: Data?, value: Data?, headers: [MessageHeader]) -> Int {
        headers.reduce((key?.count ?? 0) + (value?.count ?? 0)) { $0 + $1.key.utf8.count + ($1.value?.count ?? 0) }
    }

    public static func issues(for request: ProduceRequest, maxBytes: Int = defaultMaxMessageBytes) -> [ProduceIssue] {
        var issues: [ProduceIssue] = []
        if request.topic.isEmpty { issues.append(.emptyTopic) }
        if let partition = request.partition, partition < 0 { issues.append(.negativePartition) }
        for (index, header) in request.headers.enumerated() {
            if header.key.isEmpty {
                issues.append(.emptyHeaderKey(index: index))
            } else if header.key.utf8.contains(0) {
                issues.append(.headerKeyHasNul(index: index))
            }
        }
        if let timestamp = request.timestamp, timestamp.timeIntervalSince1970 < 0 { issues.append(.timestampBeforeEpoch) }
        let size = size(of: request)
        if size > maxBytes { issues.append(.tooLarge(size: size, limit: maxBytes)) }
        return issues
    }

    /// The error a client throws for a request that fails validation.
    public static func error(for issues: [ProduceIssue]) -> KafkaError? {
        guard let first = issues.first else { return nil }
        if case .tooLarge = first { return .clusterError(code: KafkaError.messageTooLargeCode, message: first.message) }
        return .clusterError(code: KafkaError.invalidProduceRequestCode, message: first.message)
    }

    /// "12 KB", "1 MB".
    public static func sizeText(_ bytes: Int) -> String {
        let formatter = ByteCountFormatter()
        formatter.countStyle = .binary
        formatter.allowsNonnumericFormatting = false
        return formatter.string(fromByteCount: Int64(bytes))
    }
}
