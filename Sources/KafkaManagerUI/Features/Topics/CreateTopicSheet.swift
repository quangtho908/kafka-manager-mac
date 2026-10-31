import SwiftUI
import KafkaManagerCore

/// "New Topic" form. Return creates when the form is valid; Esc cancels.
struct CreateTopicSheet: View {
    @State private var model: CreateTopicFormViewModel
    @Environment(\.dismiss) private var dismiss
    @FocusState private var nameFocused: Bool

    private enum Metrics {
        static let spacing: Double = 12
        static let padding: Double = 20
        static let width: Double = 440
        static let numberWidth: Double = 80
        static let issueSpacing: Double = 4
    }

    init(model: @autoclosure () -> CreateTopicFormViewModel) {
        _model = State(initialValue: model())
    }

    var body: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            Text("New Topic on \(model.clusterName)").font(.headline)
            if let reason = model.gate.disabledReason {
                Label(reason, systemImage: "lock.fill").foregroundStyle(.secondary)
            }
            field("Name", .name) {
                TextField("Name", text: $model.name)
                    .labelsHidden()
                    .textFieldStyle(.roundedBorder)
                    .focused($nameFocused)
                    .onSubmit(submit)
                    .onChange(of: model.name) { model.touch(.name) }
            }
            HStack(alignment: .top, spacing: Metrics.spacing) {
                field("Partitions", .partitions) {
                    TextField("Partitions", value: $model.partitions, format: .number.grouping(.never))
                        .labelsHidden()
                        .textFieldStyle(.roundedBorder)
                        .frame(width: Metrics.numberWidth)
                        .onSubmit(submit)
                        .onChange(of: model.partitions) { model.touch(.partitions) }
                }
                field("Replication factor", .replicationFactor) {
                    TextField("Replication factor", value: $model.replicationFactor, format: .number.grouping(.never))
                        .labelsHidden()
                        .textFieldStyle(.roundedBorder)
                        .frame(width: Metrics.numberWidth)
                        .onSubmit(submit)
                        .onChange(of: model.replicationFactor) { model.touch(.replicationFactor) }
                }
            }
            field("Retention (ms, optional)", .retention) {
                TextField("Broker default", text: $model.retentionMs)
                    .labelsHidden()
                    .textFieldStyle(.roundedBorder)
                    .onSubmit(submit)
                    .onChange(of: model.retentionMs) { model.touch(.retention) }
            }
            VStack(alignment: .leading, spacing: Metrics.issueSpacing) {
                Text("Cleanup policy (optional)").font(.subheadline)
                Picker("Cleanup policy", selection: $model.cleanupPolicy) {
                    Text("Broker default").tag(CleanupPolicy?.none)
                    ForEach(CleanupPolicy.allCases) { policy in
                        Text(policy.title).tag(CleanupPolicy?.some(policy))
                    }
                }
                .labelsHidden()
            }
            if case .failed(let message) = model.submitState {
                ErrorBanner(message: message)
            }
            HStack {
                if model.isCreating { ProgressView().controlSize(.small).accessibilityLabel(Text("Creating")) }
                Spacer()
                Button("Cancel", role: .cancel) { dismiss() }
                    .keyboardShortcut(.cancelAction)
                    .disabled(model.isCreating)
                Button("Create") { model.submit() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(!model.canSubmit)
                    .help(Text(model.gate.help))
            }
        }
        .padding(Metrics.padding)
        .frame(width: Metrics.width)
        .onAppear { nameFocused = true }
        .onChange(of: model.submitState) {
            if case .created = model.submitState { dismiss() }
        }
    }

    private func submit() {
        if model.canSubmit { model.submit() } else { model.touch(.name) }
    }

    private func field<Content: View>(_ title: LocalizedStringKey, _ field: CreateTopicFormViewModel.Field, @ViewBuilder content: () -> Content) -> some View {
        VStack(alignment: .leading, spacing: Metrics.issueSpacing) {
            Text(title).font(.subheadline)
            content()
            ForEach(model.issues(for: field), id: \.self) { issue in
                Label(issue.message, systemImage: issue.isError ? "exclamationmark.circle.fill" : "exclamationmark.triangle.fill")
                    .font(.caption)
                    .foregroundStyle(issue.isError ? Color.red : Color.orange)
            }
        }
    }
}
