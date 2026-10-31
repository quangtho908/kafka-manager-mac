import SwiftUI
import KafkaManagerCore

/// Three columns: connections, the selected connection's sections with its status, and a detail placeholder.
public struct ShellView: View {
    @Bindable var shell: ShellViewModel
    let sessions: ClusterSessions
    @Binding var formRequest: ConnectionFormRequest?
    let refreshToken: Int

    /// ⌘R goes to whatever is on screen: the topic list when Topics is open, otherwise the connection list.
    @State private var connectionRefresh = 0
    @State private var topicRefresh = 0
    @State private var groupRefresh = 0

    private enum Metrics {
        static let sidebarMin: Double = 240
        static let sidebarIdeal: Double = 280
        static let sidebarMax: Double = 380
        static let contentMin: Double = 300
        static let contentIdeal: Double = 380
        static let contentMax: Double = 640
    }

    public init(
        shell: ShellViewModel,
        sessions: ClusterSessions,
        formRequest: Binding<ConnectionFormRequest?>,
        refreshToken: Int
    ) {
        self.shell = shell
        self.sessions = sessions
        self._formRequest = formRequest
        self.refreshToken = refreshToken
    }

    public var body: some View {
        NavigationSplitView {
            ConnectionListView(model: shell.connections, formRequest: $formRequest, refreshToken: connectionRefresh)
                .navigationSplitViewColumnWidth(min: Metrics.sidebarMin, ideal: Metrics.sidebarIdeal, max: Metrics.sidebarMax)
        } content: {
            ClusterColumn(shell: shell, sessions: sessions, topicRefresh: topicRefresh, groupRefresh: groupRefresh)
                .navigationSplitViewColumnWidth(min: Metrics.contentMin, ideal: Metrics.contentIdeal, max: Metrics.contentMax)
        } detail: {
            DetailColumn(shell: shell)
        }
        .onChange(of: shell.connections.state) { shell.pruneSelection() }
        .onChange(of: refreshToken) {
            switch shell.selectedSection {
            case .topics: topicRefresh += 1
            case .groups: groupRefresh += 1
            case nil: connectionRefresh += 1
            }
        }
    }
}

/// Column 3: the selected topic's detail, or the placeholder for what is open.
private struct DetailColumn: View {
    let shell: ShellViewModel

    var body: some View {
        if shell.selectedSection == .topics, let list = shell.selectedTopicList, let topic = list.selection {
            TopicDetailHost(list: list, topic: topic)
        } else if shell.selectedSection == .groups, let list = shell.selectedGroupList, let group = list.selection {
            GroupDetailHost(list: list, group: group)
        } else {
            DetailPlaceholder(section: shell.selectedSection, hasConnection: shell.selectedConnection != nil)
        }
    }
}

/// Owns the detail model for the selected topic; it is fetched in a task, not in body.
private struct TopicDetailHost: View {
    let list: TopicListViewModel
    let topic: String
    @State private var model: TopicDetailViewModel?

    var body: some View {
        Group {
            if let model, model.topicName == topic {
                TopicDetailView(
                    model: model,
                    delete: (gate: list.writeGate, isBusy: list.isDeleting, action: { list.requestDelete(of: topic) })
                ).id(topic)
            } else {
                Color.clear
            }
        }
        .task(id: topic) { model = list.detailModel(for: topic) }
    }
}

/// Owns the detail model for the selected group; it is created in a task, not in body.
private struct GroupDetailHost: View {
    let list: GroupListViewModel
    let group: String
    @State private var model: GroupDetailViewModel?

    var body: some View {
        Group {
            if let model, model.groupId == group {
                GroupDetailView(model: model, reset: ResetControl(gate: list.writeGate, open: { list.requestReset(of: group, target: $0) })).id(group)
            } else {
                Color.clear
            }
        }
        .task(id: group) { model = list.detailModel(for: group) }
    }
}

private struct DetailPlaceholder: View {
    let section: ShellSection?
    let hasConnection: Bool

    var body: some View {
        if let section {
            ContentUnavailableView(section.detailPrompt, systemImage: section.symbolName)
        } else if hasConnection {
            ContentUnavailableView("Choose Topics or Consumer Groups", systemImage: "sidebar.left")
        } else {
            ContentUnavailableView("Select a connection", systemImage: "externaldrive")
        }
    }
}

/// Middle column: nothing until a connection is selected.
private struct ClusterColumn: View {
    @Bindable var shell: ShellViewModel
    let sessions: ClusterSessions
    let topicRefresh: Int
    let groupRefresh: Int

    var body: some View {
        if let connection = shell.selectedConnection {
            ConnectedColumn(connection: connection, shell: shell, sessions: sessions, topicRefresh: topicRefresh, groupRefresh: groupRefresh)
                .id(connection.id)
        } else {
            ContentUnavailableView("Select a connection", systemImage: "externaldrive")
        }
    }
}

/// Which section is open on which session; changing either re-runs the lazy connect.
private struct OpenRequest: Hashable {
    let section: ShellSection?
    let session: ObjectIdentifier?
}

private struct ActionRequest: Hashable {
    let action: SessionAction
    let nonce: Int
}

private struct ReadOnlyRequest: Hashable {
    let value: Bool
    let nonce: Int
}

private struct ConnectedColumn: View {
    let connection: ClusterConnection
    @Bindable var shell: ShellViewModel
    let sessions: ClusterSessions
    let topicRefresh: Int
    let groupRefresh: Int

    @State private var session: ClusterSession?
    @State private var topics: TopicListViewModel?
    @State private var groups: GroupListViewModel?
    @State private var actionRequest: ActionRequest?
    @State private var actionCount = 0
    @State private var readOnlyRequest: ReadOnlyRequest?

    var body: some View {
        VStack(spacing: 0) {
            banner
            if let session {
                StatusArea(session: session, onAction: request)
                Divider()
            }
            sectionPicker
            Divider()
            sectionContent
        }
        .topAlignedColumn()
        .navigationTitle(connection.name)
        // Resolved in a task, not in body; a profile edit re-resolves and the registry resets the session.
        .task(id: connection) {
            let resolved = await sessions.session(for: connection)
            session = resolved
            topics = shell.topicList(for: resolved)
            groups = shell.groupList(for: resolved)
        }
        // Opening Topics or Groups is what connects; merely selecting the connection does not.
        .task(id: OpenRequest(section: shell.selectedSection, session: session.map(ObjectIdentifier.init))) {
            if shell.selectedSection != nil, let session { _ = try? await session.ensureConnected() }
        }
        .task(id: readOnlyRequest) {
            guard let request = readOnlyRequest, let session else { return }
            await session.setReadOnly(request.value)
        }
        .task(id: actionRequest) {
            guard let request = actionRequest, let session else { return }
            switch request.action {
            case .connect: await session.connect()
            case .disconnect: await session.disconnect()
            }
        }
    }

    /// Nothing is selected until the user picks one; picking is what connects.
    private var sectionPicker: some View {
        Picker("Section", selection: $shell.selectedSection) {
            ForEach(ShellSection.allCases) { section in
                Label(section.title, systemImage: section.symbolName).tag(Optional(section))
            }
        }
        .pickerStyle(.segmented)
        .labelsHidden()
        .padding(10)
    }

    @ViewBuilder private var sectionContent: some View {
        switch shell.selectedSection {
        case .topics:
            if let topics { TopicListView(model: topics, refreshToken: topicRefresh) } else { Color.clear }
        case .groups:
            if let groups { GroupListView(model: groups, refreshToken: groupRefresh) } else { Color.clear }
        case nil:
            ContentUnavailableView("Choose Topics or Consumer Groups", systemImage: "sidebar.left")
        }
    }

    private var banner: some View {
        let isReadOnly = session?.isReadOnly ?? GuardedKafkaClient.defaultReadOnly(for: connection.environment)
        let presentation = EnvironmentBannerPresentation(
            clusterName: connection.name, environment: connection.environment, isReadOnly: isReadOnly)
        let handler: ((Bool) -> Void)? = session == nil ? nil : { requestReadOnly($0) }
        return EnvironmentBanner(presentation: presentation, onSetReadOnly: handler)
    }

    private func requestReadOnly(_ value: Bool) {
        actionCount += 1
        readOnlyRequest = ReadOnlyRequest(value: value, nonce: actionCount)
    }

    private func request(_ action: SessionAction) {
        actionCount += 1
        actionRequest = ActionRequest(action: action, nonce: actionCount)
    }
}

private struct StatusArea: View {
    let session: ClusterSession
    let onAction: (SessionAction) -> Void

    private enum Metrics {
        static let padding: Double = 10
    }

    var body: some View {
        switch session.status {
        case .failed(let message):
            ErrorBanner(message: message, onRetry: { onAction(.connect) })
        case .idle, .connecting, .connected:
            HStack {
                Label(session.status.label, systemImage: session.status.symbolName)
                    .foregroundStyle(session.status == .connected ? Color.green : Color.secondary)
                    .accessibilityElement(children: .combine)
                if session.status == .connecting {
                    ProgressView().controlSize(.small).accessibilityLabel(Text("Connecting"))
                }
                Spacer()
                Button(session.status.action.title) { onAction(session.status.action) }
            }
            .padding(Metrics.padding)
        }
    }
}
