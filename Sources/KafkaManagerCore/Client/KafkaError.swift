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
        case .clusterError(let code, _):
            String(localized: "The cluster returned an error (code \(code)).")
        }
    }
}
