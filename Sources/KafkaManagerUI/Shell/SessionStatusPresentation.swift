import KafkaManagerCore

/// What the status row offers for a session state.
public enum SessionAction: Sendable, Equatable {
    case connect
    case disconnect

    public var title: String {
        switch self {
        case .connect: String(localized: "Connect")
        case .disconnect: String(localized: "Disconnect")
        }
    }
}

public extension ClusterSession.Status {
    var label: String {
        switch self {
        case .idle: String(localized: "Not connected")
        case .connecting: String(localized: "Connecting…")
        case .connected: String(localized: "Connected")
        case .failed: String(localized: "Connection failed")
        }
    }

    var symbolName: String {
        switch self {
        case .idle: "circle"
        case .connecting: "arrow.triangle.2.circlepath"
        case .connected: "checkmark.circle.fill"
        case .failed: "xmark.octagon.fill"
        }
    }

    /// The button next to the status: connect when there is no connection, disconnect when there is or is being made.
    var action: SessionAction {
        switch self {
        case .idle, .failed: .connect
        case .connecting, .connected: .disconnect
        }
    }
}
