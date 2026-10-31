import Foundation

public enum KafkaError: Error, Sendable, Equatable {
    case unreachable(host: String)
    case authenticationFailed
    case authorizationFailed(resource: String)
    case timeout
    case unknownTopic(String)
    case topicAlreadyExists(String)
    case readOnly(clusterName: String)
    /// Anything else; `message` is the original broker/client text, kept for diagnostics only.
    case clusterError(code: Int, message: String)

    /// Kafka protocol error codes with their own wording: INVALID_TOPIC_EXCEPTION, INVALID_PARTITIONS,
    /// INVALID_REPLICATION_FACTOR.
    public static let invalidTopicCode = 17
    public static let invalidPartitionsCode = 37
    public static let invalidReplicationFactorCode = 38
    /// MSG_SIZE_TOO_LARGE (10), from the broker or from the client's own size check.
    public static let messageTooLargeCode = 10
    /// A request the app refused to send; `message` says why (see `ProduceIssue`).
    public static let invalidProduceRequestCode = -3

    /// Plain-language text safe to show in the UI.
    public var userMessage: String {
        switch self {
        case .unreachable(let host):
            String(localized: "Can't reach \(host). Check the address, your network, and that the broker is running.")
        case .authenticationFailed:
            String(localized: "Authentication failed. Check the username, password, SASL mechanism, and certificates.")
        case .authorizationFailed(let resource):
            String(localized: "You don't have permission to access \(resource).")
        case .timeout:
            String(localized: "The cluster took too long to respond. Try again.")
        case .unknownTopic(let name):
            String(localized: "Topic \"\(name)\" doesn't exist.")
        case .topicAlreadyExists(let name):
            String(localized: "Topic \"\(name)\" already exists.")
        case .readOnly(let clusterName):
            String(localized: "\"\(clusterName)\" is read-only. Switch the cluster to a writable environment to make changes.")
        case .clusterError(let code, _) where code == Self.invalidTopicCode:
            String(localized: "The cluster rejected the topic name.")
        case .clusterError(let code, _) where code == Self.invalidPartitionsCode:
            String(localized: "The number of partitions is invalid. Use 1 or more.")
        case .clusterError(let code, _) where code == Self.invalidReplicationFactorCode:
            String(localized: "The replication factor is invalid. Use 1 or more, and no more than the number of brokers.")
        case .clusterError(let code, _) where code == Self.messageTooLargeCode:
            String(localized: "The message is larger than the cluster accepts. Make it smaller and try again.")
        case .clusterError(let code, let message) where code == Self.invalidProduceRequestCode:
            message
        case .clusterError where isNonEmptyGroup:
            OffsetResetRule.activeMembersMessage
        case .clusterError where isUnknownGroup:
            String(localized: "That consumer group doesn't exist. It may have been deleted.")
        case .clusterError(let code, _):
            String(localized: "The cluster returned an error (code \(code)).")
        }
    }
}
