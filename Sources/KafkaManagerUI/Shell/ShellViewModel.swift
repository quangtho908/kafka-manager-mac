import Foundation
import Observation
import KafkaManagerCore

/// What a selected cluster offers in the middle column.
public enum ShellSection: String, CaseIterable, Identifiable, Sendable {
    case topics
    case groups

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .topics: String(localized: "Topics")
        case .groups: String(localized: "Consumer Groups")
        }
    }

    public var symbolName: String {
        switch self {
        case .topics: "list.bullet"
        case .groups: "person.3"
        }
    }

    /// Shown in the detail column until a real item is picked.
    public var detailPrompt: String {
        switch self {
        case .topics: String(localized: "Select a topic")
        case .groups: String(localized: "Select a group")
        }
    }
}

/// Selection state of the three-column shell. The selected connection is the list's own selection, checked against
/// the loaded list so a deleted or vanished profile reads as "none"; the section belongs to the connection it was
/// picked for, so switching connection starts with no section (and therefore no connect).
@MainActor @Observable
public final class ShellViewModel {
    public let connections: ConnectionsViewModel

    private var section: ShellSection?
    private var sectionOwner: UUID?

    public init(connections: ConnectionsViewModel) {
        self.connections = connections
    }

    public var selectedConnection: ClusterConnection? {
        guard let id = connections.selection else { return nil }
        return connections.state.value?.first { $0.id == id }
    }

    public var selectedSection: ShellSection? {
        get {
            guard let selected = selectedConnection, sectionOwner == selected.id else { return nil }
            return section
        }
        set {
            section = newValue
            sectionOwner = selectedConnection?.id
        }
    }

    /// Clears a selection whose profile is gone, once the list is known. Does nothing while loading.
    public func pruneSelection() {
        guard connections.selection != nil, connections.state.value != nil, selectedConnection == nil else { return }
        connections.selection = nil
    }
}
