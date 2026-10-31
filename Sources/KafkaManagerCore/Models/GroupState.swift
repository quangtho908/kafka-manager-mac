import Foundation

/// A consumer group's state as the broker reports it. Names the app doesn't know are kept, not dropped.
public enum GroupState: Hashable, Sendable {
    case stable
    case preparingRebalance
    case completingRebalance
    case empty
    case dead
    /// Any other name, as received (new broker versions, or "Unknown").
    case other(String)

    /// Parses the broker's name ("Stable", "PreparingRebalance", …) ignoring case and surrounding spaces.
    public init(kafkaName: String) {
        switch kafkaName.trimmingCharacters(in: .whitespaces).lowercased() {
        case "stable": self = .stable
        case "preparingrebalance": self = .preparingRebalance
        case "completingrebalance": self = .completingRebalance
        case "empty": self = .empty
        case "dead": self = .dead
        default: self = .other(kafkaName)
        }
    }

    /// The group is in the middle of a rebalance (either phase).
    public var isRebalancing: Bool { self == .preparingRebalance || self == .completingRebalance }
}

extension GroupSummary {
    public var groupState: GroupState { GroupState(kafkaName: state) }
}

extension GroupDetail {
    public var groupState: GroupState { GroupState(kafkaName: state) }
}
