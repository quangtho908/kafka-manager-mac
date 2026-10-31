import SwiftUI
import KafkaManagerCore

/// Three columns: connections, the selected connection's sections with its status, and a detail placeholder.
public struct ShellView: View {
    @Bindable var shell: ShellViewModel
    let sessions: ClusterSessions
    @Binding var formRequest: ConnectionFormRequest?
    let refreshToken: Int

    private enum Metrics {
        static let sidebarMin: Double = 240
        static let sidebarIdeal: Double = 280
        static let sidebarMax: Double = 380
        static let contentMin: Double = 220
        static let contentIdeal: Double = 260
        static let contentMax: Double = 340
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
            ConnectionListView(model: shell.connections, formRequest: $formRequest, refreshToken: refreshToken)
                .navigationSplitViewColumnWidth(min: Metrics.sidebarMin, ideal: Metrics.sidebarIdeal, max: Metrics.sidebarMax)
        } content: {
            ClusterColumn(shell: shell, sessions: sessions)
                .navigationSplitViewColumnWidth(min: Metrics.contentMin, ideal: Metrics.contentIdeal, max: Metrics.contentMax)
        } detail: {
            DetailPlaceholder(section: shell.selectedSection, hasConnection: shell.selectedConnection != nil)
        }
        .onChange(of: shell.connections.state) { shell.pruneSelection() }
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

    var body: some View {
        if let connection = shell.selectedConnection {
            ConnectedColumn(connection: connection, shell: shell, sessions: sessions)
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

private struct ConnectedColumn: View {
    let connection: ClusterConnection
    @Bindable var shell: ShellViewModel
    let sessions: ClusterSessions

    @State private var session: ClusterSession?
    @State private var actionRequest: ActionRequest?
    @State private var actionCount = 0

    var body: some View {
        VStack(spacing: 0) {
            if let session {
                StatusArea(session: session, onAction: request)
                Divider()
            }
            List(selection: $shell.selectedSection) {
                ForEach(ShellSection.allCases) { section in
                    Label(section.title, systemImage: section.symbolName).tag(section)
                }
            }
        }
        .navigationTitle(connection.name)
        // Resolved in a task, not in body; a profile edit re-resolves and the registry resets the session.
        .task(id: connection) { session = await sessions.session(for: connection) }
        // Opening Topics or Groups is what connects; merely selecting the connection does not.
        .task(id: OpenRequest(section: shell.selectedSection, session: session.map(ObjectIdentifier.init))) {
            if shell.selectedSection != nil, let session { _ = try? await session.ensureConnected() }
        }
        .task(id: actionRequest) {
            guard let request = actionRequest, let session else { return }
            switch request.action {
            case .connect: await session.connect()
            case .disconnect: await session.disconnect()
            }
        }
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
