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
    /// One topic list per cluster profile, so its list, search and sort survive switching sections or clusters. Editing
    /// the profile starts a fresh list, since it may now point at another cluster.
    @ObservationIgnored private var topicLists: [UUID: (connection: ClusterConnection, model: TopicListViewModel)] = [:]

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

    public func topicList(for session: ClusterSession) -> TopicListViewModel {
        let connection = session.connection
        if let existing = topicLists[connection.id], existing.connection == connection, existing.model.belongs(to: session) {
            return existing.model
        }
        let created = TopicListViewModel(session: session)
        topicLists[connection.id] = (connection, created)
        return created
    }

    /// Clears a selection whose profile is gone, once the list is known. Does nothing while loading.
    public func pruneSelection() {
        if let loaded = connections.state.value {
            let ids = Set(loaded.map(\.id))
            topicLists = topicLists.filter { ids.contains($0.key) }
        }
        guard connections.selection != nil, connections.state.value != nil, selectedConnection == nil else { return }
        connections.selection = nil
    }
}
