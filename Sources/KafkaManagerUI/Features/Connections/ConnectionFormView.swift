import SwiftUI
import KafkaManagerCore

/// Owns the form's view model for as long as the sheet is up. The sheet closure that builds this host is re-run
/// whenever the presenting view re-evaluates; `@State` keeps the first model for this host's identity (the sheet
/// item), so typing, `hasStoredPassword` and an in-flight save survive. The extra models built by those re-runs
/// are discarded unused; they do no I/O.
struct ConnectionFormHost: View {
    @State private var form: ConnectionFormViewModel
    let onSaved: (ClusterConnection) -> Void

    init(mode: ConnectionFormViewModel.Mode, connections: ConnectionsViewModel) {
        _form = State(initialValue: connections.makeFormViewModel(mode: mode))
        onSaved = connections.didSave
    }

    var body: some View {
        ConnectionFormView(model: form, onSaved: onSaved)
    }
}

/// Add / edit / duplicate form, presented in a sheet. Validation messages sit under their field.
struct ConnectionFormView: View {
    @Bindable var model: ConnectionFormViewModel
    let onSaved: (ClusterConnection) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var touched: Set<ValidationIssue.Field> = []
    @State private var saveRequest = 0

    private enum Metrics {
        static let width: Double = 480
        static let issueSpacing: Double = 4
        static let footerSpacing: Double = 12
        static let footerPadding: Double = 16
    }

    var body: some View {
        let issues = model.issues.visible(touched: touched)
        VStack(spacing: 0) {
            Form {
                Section {
                    TextField("Name", text: $model.name, prompt: Text("Production EU"))
                        .onChange(of: model.name) { touched.insert(.name) }
                    IssueList(issues: issues.issues(for: .name))
                    TextField("Bootstrap servers", text: $model.bootstrapServers, prompt: Text("host:9092, host2:9092"))
                        .onChange(of: model.bootstrapServers) { touched.insert(.bootstrapServers) }
                    IssueList(issues: issues.issues(for: .bootstrapServers))
                    Picker("Environment", selection: $model.environment) {
                        ForEach(ClusterConnection.Environment.allCases, id: \.self) { Text($0.label).tag($0) }
                    }
                }
                Section("Security") {
                    Picker("Security", selection: $model.security) {
                        ForEach(ClusterConnection.Security.allCases, id: \.self) { Text($0.label).tag($0) }
                    }
                    .onChange(of: model.security) {
                        if model.usesSASL { touched.formUnion([.username, .saslMechanism]) }
                    }
                    if model.usesSASL { saslFields(issues: issues) }
                }
            }
            .formStyle(.grouped)
            Divider()
            footer
        }
        .frame(width: Metrics.width)
        .task { await model.loadStoredPasswordFlag() }
        .task(id: saveRequest) {
            if saveRequest > 0 { await model.save() }
        }
        .onChange(of: model.saveState) { _, state in
            if case .saved(let connection) = state {
                onSaved(connection)
                dismiss()
            }
        }
    }

    // MARK: SASL

    @ViewBuilder
    private func saslFields(issues: [ValidationIssue]) -> some View {
        Picker("SASL mechanism", selection: $model.saslMechanism) {
            Text("Choose…").tag(ClusterConnection.SASLMechanism?.none)
            ForEach(ClusterConnection.SASLMechanism.allCases, id: \.self) {
                Text($0.label).tag(ClusterConnection.SASLMechanism?.some($0))
            }
        }
        .onChange(of: model.saslMechanism) { touched.insert(.saslMechanism) }
        IssueList(issues: issues.issues(for: .saslMechanism))
        TextField("Username", text: $model.username)
            .onChange(of: model.username) { touched.insert(.username) }
        IssueList(issues: issues.issues(for: .username))
        SecureField("Password", text: $model.password, prompt: Text(passwordPrompt))
            .disabled(model.clearStoredPassword)
        if model.isEditing && model.hasStoredPassword {
            Toggle("Remove saved password", isOn: $model.clearStoredPassword)
        }
    }

    private var passwordPrompt: String {
        model.isEditing && model.hasStoredPassword
            ? String(localized: "Saved – leave empty to keep")
            : String(localized: "Optional")
    }

    // MARK: Footer

    /// Why Save is disabled, even for fields the user hasn't touched yet.
    private var saveHint: String? {
        guard !model.canSave, model.saveState != .saving else { return nil }
        return model.issues.first { $0.severity == .error }?.message
    }

    private var footer: some View {
        HStack(spacing: Metrics.footerSpacing) {
            if case .failed(let message) = model.saveState {
                Label(message, systemImage: "xmark.octagon.fill")
                    .foregroundStyle(.red)
                    .font(.callout)
                    .accessibilityLabel(Text("Couldn't save: \(message)"))
            } else if let hint = saveHint {
                Text(hint)
                    .foregroundStyle(.secondary)
                    .font(.callout)
            }
            Spacer()
            if model.saveState == .saving {
                ProgressView().controlSize(.small)
            }
            Button("Cancel", role: .cancel) { dismiss() }
                .keyboardShortcut(.cancelAction)
            Button("Save") { saveRequest += 1 }
                .keyboardShortcut(.defaultAction)
                .disabled(!model.canSave)
        }
        .padding(Metrics.footerPadding)
    }
}

/// Inline messages for one field: icon + text, red for errors and orange for warnings.
private struct IssueList: View {
    let issues: [ValidationIssue]

    var body: some View {
        ForEach(issues, id: \.self) { issue in
            Label {
                Text(issue.message)
            } icon: {
                Image(systemName: issue.severity == .error ? "xmark.octagon.fill" : "exclamationmark.triangle.fill")
            }
            .foregroundStyle(issue.severity == .error ? Color.red : Color.orange)
            .font(.callout)
            .accessibilityElement(children: .combine)
            .accessibilityLabel(Text("\(issue.severity == .error ? "Error" : "Warning"): \(issue.message)"))
        }
    }
}
