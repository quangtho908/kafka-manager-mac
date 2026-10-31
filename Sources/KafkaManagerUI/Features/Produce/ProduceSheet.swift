import SwiftUI
import KafkaManagerCore

/// Compose and send one message to a topic. Send is ⌘Return; on a production cluster it asks for the topic name first.
struct ProduceSheet: View {
    @Bindable var model: ProduceFormViewModel
    @Environment(\.dismiss) private var dismiss

    private enum Metrics {
        static let spacing: Double = 12
        static let fieldSpacing: Double = 4
        static let padding: Double = 16
        static let minWidth: Double = 640
        static let minHeight: Double = 580
        static let encodingWidth: Double = 100
        static let partitionWidth: Double = 160
        static let editorMinHeight: Double = 150
        static let numberWidth: Double = 80
        static let kindWidth: Double = 80
        static let headerKeyWidth: Double = 180
    }

    var body: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            title
            ScrollView {
                VStack(alignment: .leading, spacing: Metrics.spacing) {
                    partition
                    key
                    value
                    headers
                    timestamp
                    repeatCount
                }
                .padding(.trailing, Metrics.spacing)
                .frame(maxWidth: .infinity, alignment: .leading)
                .disabled(model.isSending)
            }
            Divider()
            outcome
            buttons
        }
        .padding(Metrics.padding)
        .frame(minWidth: Metrics.minWidth, minHeight: Metrics.minHeight)
        .topAlignedColumn()
        .sheet(item: $model.pendingConfirmation) { confirmation in
            DestructiveConfirmationSheet(confirmation: confirmation, gate: model.writeGate) { model.confirmSend() }
        }
    }

    // MARK: Header

    private var title: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text("Produce Message").font(.title2.weight(.semibold))
            Text(model.topicName).font(.callout).foregroundStyle(.secondary).textSelection(.enabled).lineLimit(1).truncationMode(.middle)
            if let reason = model.writeGate.disabledReason {
                Label(reason, systemImage: "lock.fill").font(.callout).foregroundStyle(.secondary)
            }
        }
    }

    // MARK: Fields

    private var partition: some View {
        field("Partition", issues: model.issues(for: .partition)) {
            Picker("Partition", selection: $model.partition) {
                Text("Auto").tag(Int?.none)
                ForEach(model.partitionIDs, id: \.self) { id in
                    Text("Partition \(id)").tag(Optional(id))
                }
            }
            .labelsHidden()
            .frame(width: Metrics.partitionWidth)
            if let original = model.originalPartition {
                Text("Copied from partition \(original). Auto lets the partitioner choose.")
                    .font(.caption).foregroundStyle(.secondary)
            }
        }
    }

    private var key: some View {
        field("Key", issues: model.issues(for: .key)) {
            HStack {
                TextField("No key", text: $model.keyText)
                    .labelsHidden()
                    .textFieldStyle(.roundedBorder)
                    .font(.body.monospaced())
                    .autocorrectionDisabled()
                encodingPicker("Key encoding", selection: $model.keyEncoding, choices: ProduceEncoding.keyChoices)
            }
        }
    }

    private var value: some View {
        field("Value", issues: model.issues(for: .value)) {
            HStack {
                Toggle("Null (tombstone)", isOn: $model.isTombstone).toggleStyle(.checkbox)
                Spacer()
                if model.valueEncoding == .json, !model.isTombstone {
                    Button("Format") { model.formatValue() }
                        .controlSize(.small)
                        .disabled(!model.canFormatValue)
                        .help(Text("Re-indent the JSON"))
                }
                encodingPicker("Value encoding", selection: $model.valueEncoding, choices: ProduceEncoding.allCases)
                    .disabled(model.isTombstone)
            }
            TextEditor(text: $model.valueText)
                .font(.body.monospaced())
                .autocorrectionDisabled()
                .scrollContentBackground(.hidden)
                .padding(4)
                .frame(minHeight: Metrics.editorMinHeight)
                .background(.background, in: RoundedRectangle(cornerRadius: 6))
                .overlay(RoundedRectangle(cornerRadius: 6).stroke(.separator))
                .opacity(model.isTombstone ? 0.4 : 1)
                .disabled(model.isTombstone)
                .accessibilityLabel(Text("Value"))
            if let size = model.sizeText {
                Label(size, systemImage: model.isOverSizeLimit ? "exclamationmark.triangle.fill" : "scalemass")
                    .font(.caption)
                    .foregroundStyle(model.isOverSizeLimit ? Color.red : Color.secondary)
                    .monospacedDigit()
            }
        }
    }

    private var headers: some View {
        field("Headers", issues: model.issues(for: .headers)) {
            ForEach($model.headers) { $header in
                HStack {
                    TextField("Name", text: $header.key)
                        .labelsHidden()
                        .textFieldStyle(.roundedBorder)
                        .frame(width: Metrics.headerKeyWidth)
                        .accessibilityLabel(Text("Header name"))
                    TextField(header.kind == .null ? "null" : "Value", text: $header.value)
                        .labelsHidden()
                        .textFieldStyle(.roundedBorder)
                        .font(.body.monospaced())
                        .disabled(header.kind == .null)
                        .accessibilityLabel(Text("Header value"))
                    Picker("Header value encoding", selection: $header.kind) {
                        ForEach(HeaderDraft.Kind.allCases, id: \.self) { Text($0.title).tag($0) }
                    }
                    .labelsHidden()
                    .frame(width: Metrics.kindWidth)
                    Button {
                        model.removeHeader(header.id)
                    } label: {
                        Label("Remove header", systemImage: "minus.circle").labelStyle(.iconOnly)
                    }
                    .buttonStyle(.borderless)
                }
            }
            Button {
                model.addHeader()
            } label: {
                Label("Add Header", systemImage: "plus")
            }
            .controlSize(.small)
        }
    }

    private var timestamp: some View {
        field("Timestamp", issues: model.issues(for: .timestamp)) {
            HStack {
                Toggle("Set the timestamp", isOn: $model.useTimestamp).toggleStyle(.checkbox)
                if model.useTimestamp {
                    DatePicker("Timestamp", selection: $model.timestamp).labelsHidden()
                } else {
                    Text("Uses the time of sending").font(.caption).foregroundStyle(.secondary)
                }
            }
        }
    }

    private var repeatCount: some View {
        field("Repeat", issues: model.issues(for: .repeatCount)) {
            HStack {
                TextField("Times", value: $model.repeatCount, format: .number.grouping(.never))
                    .labelsHidden()
                    .textFieldStyle(.roundedBorder)
                    .frame(width: Metrics.numberWidth)
                    .monospacedDigit()
                    .accessibilityLabel(Text("Number of times to send"))
                Stepper("Times", value: $model.repeatCount, in: 1...ProduceFormViewModel.maxRepeat).labelsHidden()
                Text(model.repeatCount == 1 ? "time" : "times").foregroundStyle(.secondary)
            }
        }
    }

    // MARK: Pieces

    private func field<Content: View>(_ title: LocalizedStringKey, issues: [ProduceFormIssue], @ViewBuilder content: () -> Content) -> some View {
        VStack(alignment: .leading, spacing: Metrics.fieldSpacing) {
            Text(title).font(.subheadline.weight(.medium))
            content()
            ForEach(issues, id: \.self) { issue in
                Label(issue.message, systemImage: "exclamationmark.circle.fill").font(.caption).foregroundStyle(.red)
            }
        }
    }

    private func encodingPicker(_ label: LocalizedStringKey, selection: Binding<ProduceEncoding>, choices: [ProduceEncoding]) -> some View {
        Picker(label, selection: selection) {
            ForEach(choices, id: \.self) { Text($0.title).tag($0) }
        }
        .labelsHidden()
        .frame(width: Metrics.encodingWidth)
    }

    // MARK: Outcome and buttons

    @ViewBuilder private var outcome: some View {
        switch model.state {
        case .idle, .sending:
            EmptyView()
        case .sent(let summary):
            VStack(alignment: .leading, spacing: 2) {
                Label(summary.text, systemImage: "checkmark.circle.fill").foregroundStyle(.green).textSelection(.enabled)
                if summary.wasStopped {
                    Text("Stopped after \(summary.sent) of \(summary.requested).").font(.caption).foregroundStyle(.secondary)
                }
            }
        case .failed(let message):
            ErrorBanner(message: message, onDismiss: { model.dismissFailure() })
        }
    }

    private var buttons: some View {
        HStack {
            if let progress = model.progress {
                ProgressView().controlSize(.small).accessibilityLabel(Text("Sending"))
                if progress.total > 1 {
                    Text("Sending \(progress.done) of \(progress.total)…").monospacedDigit().foregroundStyle(.secondary)
                    Button("Stop") { model.stop() }
                } else {
                    Text("Sending…").foregroundStyle(.secondary)
                }
            }
            Spacer()
            Button("Close") { dismiss() }
                .keyboardShortcut(.cancelAction)
                .disabled(model.isSending)
            if case .sent = model.state {
                Button("Send Another") { model.sendAnother() }
                    .keyboardShortcut(.return, modifiers: .command)
            } else {
                Button("Send") { model.requestSend() }
                    .keyboardShortcut(.return, modifiers: .command)
                    .disabled(!model.canSend)
                    .help(Text(model.sendDisabledReason ?? String(localized: "Send the message")))
            }
        }
    }
}
