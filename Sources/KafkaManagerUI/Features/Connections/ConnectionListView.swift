import SwiftUI
import KafkaManagerCore

/// Sidebar list of saved connections with New / Edit / Duplicate / Delete.
public struct ConnectionListView: View {
    @Bindable var model: ConnectionsViewModel
    @Binding var formRequest: ConnectionFormRequest?
    /// Bumped by a menu command to ask for a reload.
    let refreshToken: Int

    @State private var pendingDelete: DeleteConfirmation?
    @State private var deletionRequest: UUID?
    @State private var reloadCount = 0
    @State private var showDeleteError = false

    public init(model: ConnectionsViewModel, formRequest: Binding<ConnectionFormRequest?>, refreshToken: Int = 0) {
        self.model = model
        self._formRequest = formRequest
        self.refreshToken = refreshToken
    }

    public var body: some View {
        VStack(spacing: 0) {
            if let backup = model.recoveredBackup {
                RecoveryBanner(backup: backup, onDismiss: model.dismissRecoveryNotice)
            }
            content
        }
        .toolbar {
            ToolbarItem {
                Button("New Connection", systemImage: "plus", action: showNewForm)
                    .help("New Connection")
            }
        }
        .task(id: reloadCount + refreshToken) {
            await model.load(refresh: reloadCount + refreshToken > 0)
        }
        .task(id: deletionRequest) {
            guard let id = deletionRequest else { return }
            await model.delete(id: id)
            deletionRequest = nil
        }
        .sheet(item: $formRequest) { request in
            ConnectionFormHost(mode: request.mode, connections: model)
        }
        .sheet(item: $pendingDelete) { confirmation in
            DeleteConnectionSheet(confirmation: confirmation) { deletionRequest = confirmation.connection.id }
        }
        .onChange(of: model.deleteError) { _, message in showDeleteError = message != nil }
        .alert("Couldn't delete connection", isPresented: $showDeleteError) {
            Button("OK") { model.dismissDeleteError() }
        } message: {
            Text(model.deleteError ?? "")
        }
    }

    private var content: some View {
        StateView(
            state: model.state,
            isEmpty: \.isEmpty,
            loadingTitle: String(localized: "Loading connections…"),
            failureTitle: String(localized: "Couldn't load connections"),
            emptyTitle: String(localized: "No connections yet"),
            emptyMessage: String(localized: "Add a Kafka cluster to get started."),
            emptySymbol: "externaldrive.badge.plus",
            retry: { reloadCount += 1 },
            emptyActions: { Button("New Connection", action: showNewForm) },
            content: list)
    }

    private func list(_ connections: [ClusterConnection]) -> some View {
        List(connections, selection: $model.selection) { connection in
            ConnectionRow(connection: connection)
        }
        .contextMenu(forSelectionType: UUID.self) { ids in
            if let connection = connection(for: ids, in: connections) {
                Button("Edit…") { formRequest = ConnectionFormRequest(mode: .edit(connection)) }
                Button("Duplicate…") { formRequest = ConnectionFormRequest(mode: model.duplicateDraft(of: connection)) }
                Divider()
                Button("Delete…", role: .destructive) { pendingDelete = DeleteConfirmation(connection: connection) }
            }
        } primaryAction: { ids in
            if let connection = connection(for: ids, in: connections) {
                formRequest = ConnectionFormRequest(mode: .edit(connection))
            }
        }
    }

    private func connection(for ids: Set<UUID>, in connections: [ClusterConnection]) -> ClusterConnection? {
        guard let id = ids.first else { return nil }
        return connections.first { $0.id == id }
    }

    private func showNewForm() {
        formRequest = ConnectionFormRequest(mode: .new)
    }
}

private struct ConnectionRow: View {
    let connection: ClusterConnection

    private enum Metrics {
        static let spacing: Double = 2
        static let verticalPadding: Double = 2
    }

    var body: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            HStack {
                Text(connection.name).font(.headline).lineLimit(1)
                Spacer()
                EnvironmentBadge(environment: connection.environment)
            }
            Text(HostSummary.make(connection.bootstrapServers))
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(1)
        }
        .padding(.vertical, Metrics.verticalPadding)
        .accessibilityElement(children: .combine)
    }
}

/// Environment label with icon; the text is always shown so the color is never the only cue.
struct EnvironmentBadge: View {
    let environment: ClusterConnection.Environment

    var body: some View {
        Label(environment.label, systemImage: environment.symbolName)
            .font(.caption)
            .foregroundStyle(color)
    }

    private var color: Color {
        switch environment.tone {
        case .calm: .green
        case .caution: .orange
        case .danger: .red
        }
    }
}

private struct RecoveryBanner: View {
    let backup: URL
    let onDismiss: () -> Void

    private enum Metrics {
        static let padding: Double = 10
    }

    var body: some View {
        HStack {
            Label("The saved connections file was damaged and has been reset. A copy was kept as \(backup.lastPathComponent).",
                  systemImage: "exclamationmark.triangle.fill")
                .font(.callout)
            Spacer()
            Button("Dismiss", action: onDismiss)
        }
        .padding(Metrics.padding)
        .background(.orange.opacity(0.2))
        .accessibilityElement(children: .contain)
    }
}

private struct DeleteConnectionSheet: View {
    let confirmation: DeleteConfirmation
    let onConfirm: () -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var typedName = ""

    private enum Metrics {
        static let width: Double = 420
        static let spacing: Double = 12
        static let padding: Double = 20
    }

    var body: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            Label("Delete “\(confirmation.connection.name)”?", systemImage: "trash")
                .font(.headline)
            Text("This removes the connection and its saved password from this Mac. The Kafka cluster itself is not changed.")
                .foregroundStyle(.secondary)
            if confirmation.requiresTypedName {
                EnvironmentBadge(environment: .production)
                Text("This is a Production connection. Type its name to confirm.")
                TextField("Connection name", text: $typedName, prompt: Text(confirmation.connection.name))
                    .onSubmit(confirmIfAllowed)
            }
            HStack {
                Spacer()
                // Return must never delete: Cancel is the default button; production confirms through the typed name.
                Button("Delete", role: .destructive, action: confirmIfAllowed)
                    .disabled(!confirmation.isSatisfied(by: typedName))
                Button("Cancel", role: .cancel) { dismiss() }
                    .keyboardShortcut(.defaultAction)
            }
        }
        .padding(Metrics.padding)
        .frame(width: Metrics.width)
    }

    private func confirmIfAllowed() {
        guard confirmation.isSatisfied(by: typedName) else { return }
        onConfirm()
        dismiss()
    }
}
