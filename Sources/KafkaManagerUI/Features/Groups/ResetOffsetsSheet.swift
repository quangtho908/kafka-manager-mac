import SwiftUI
import KafkaManagerCore

/// How one preview row reads: words and an icon together, never colour alone.
struct OffsetRowStatus {
    let label: String
    let symbolName: String

    init(_ row: OffsetResetPreviewRow) {
        switch (row.willChange, row.isClamped) {
        case (true, true): self.init(label: String(localized: "Changed, clamped"), symbolName: "exclamationmark.triangle")
        case (true, false): self.init(label: String(localized: "Changed"), symbolName: "arrow.right.circle")
        case (false, true): self.init(label: String(localized: "Unchanged, clamped"), symbolName: "exclamationmark.triangle")
        case (false, false): self.init(label: String(localized: "Unchanged"), symbolName: "equal.circle")
        }
    }

    private init(label: String, symbolName: String) {
        self.label = label
        self.symbolName = symbolName
    }
}

/// Target and scope on top, the live preview below; nothing is written until Apply and the typed confirmation.
struct ResetOffsetsSheet: View {
    @Bindable var model: OffsetResetViewModel
    @Environment(\.dismiss) private var dismiss

    private enum Metrics {
        static let spacing: Double = 10
        static let padding: Double = 16
        static let minWidth: Double = 720
        static let minHeight: Double = 520
        static let offsetFieldWidth: Double = 160
        static let numberWidth: Double = 90
        static let partitionWidth: Double = 70
        static let statusWidth: Double = 150
        static let topicRowsHeight: Double = 70
    }

    var body: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            title
            options
            Divider()
            previewArea
            outcome
            buttons
        }
        .padding(Metrics.padding)
        .frame(minWidth: Metrics.minWidth, minHeight: Metrics.minHeight)
        .topAlignedColumn()
        .task(id: model.previewKey) {
            // Typing and date changes settle before the broker is asked.
            if model.preview.value != nil { try? await Task.sleep(for: .milliseconds(250)) }
            guard !Task.isCancelled else { return }
            await model.refreshPreview()
        }
        .sheet(item: $model.pendingConfirmation) { confirmation in
            DestructiveConfirmationSheet(confirmation: confirmation, gate: model.writeGate) { model.confirmApply() }
        }
    }

    private var title: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text("Reset Offsets").font(.title2.weight(.semibold))
            Text(model.groupId).font(.callout).foregroundStyle(.secondary).textSelection(.enabled).lineLimit(1).truncationMode(.middle)
        }
    }

    private var options: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            HStack(spacing: Metrics.spacing) {
                Picker("Move to", selection: $model.targetKind) {
                    ForEach(OffsetTargetKind.allCases) { Text($0.title).tag($0) }
                }
                .fixedSize()
                switch model.targetKind {
                case .time:
                    DatePicker("Time", selection: $model.time).labelsHidden()
                case .offset:
                    TextField("Offset", text: $model.offsetText)
                        .textFieldStyle(.roundedBorder)
                        .frame(width: Metrics.offsetFieldWidth)
                        .monospacedDigit()
                case .latest, .earliest:
                    EmptyView()
                }
                Spacer()
            }
            if let issue = model.targetIssue {
                Label(issue, systemImage: "exclamationmark.triangle").font(.callout).foregroundStyle(.orange)
            }
            if model.availableTopics.count > 1 {
                Toggle("Only these topics", isOn: $model.onlySelectedTopics)
                if model.onlySelectedTopics {
                    ScrollView {
                        VStack(alignment: .leading, spacing: 2) {
                            ForEach(model.availableTopics, id: \.self) { topic in
                                Toggle(topic, isOn: topicBinding(topic))
                            }
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)
                    }
                    .frame(maxHeight: Metrics.topicRowsHeight)
                }
            }
        }
    }

    private func topicBinding(_ topic: String) -> Binding<Bool> {
        Binding(
            get: { model.selectedTopics.contains(topic) },
            set: { isOn in
                if isOn { model.selectedTopics.insert(topic) } else { model.selectedTopics.remove(topic) }
            })
    }

    @ViewBuilder private var previewArea: some View {
        switch model.preview {
        case .idle, .loading:
            HStack { ProgressView().controlSize(.small); Text("Loading the group…") }
                .accessibilityElement(children: .combine)
            Spacer(minLength: 0)
        case .failed(let message, _):
            ErrorBanner(message: message, onRetry: { Task { await model.reload() } })
            Spacer(minLength: 0)
        case .loaded:
            if let scoped = model.scopedPreview { loaded(scoped) }
        }
    }

    @ViewBuilder private func loaded(_ preview: OffsetResetPreview) -> some View {
        HStack(spacing: Metrics.spacing) {
            Text(totalsText(preview)).font(.callout.weight(.semibold)).monospacedDigit()
            if model.isRefreshing { ProgressView().controlSize(.small).accessibilityLabel(Text("Updating the preview")) }
            Spacer()
            if preview.clampedCount > 0 {
                Label("\(preview.clampedCount) clamped to the retained range", systemImage: "exclamationmark.triangle")
                    .font(.caption).foregroundStyle(.secondary)
            }
        }
        if preview.rows.isEmpty {
            ContentUnavailableView("No partitions", systemImage: "tray", description: Text("This group has no committed offsets or assignments to reset."))
        } else {
            Table(preview.rows) {
                TableColumn("Topic") { Text($0.topic).lineLimit(1).truncationMode(.middle) }
                TableColumn("Partition") { Text($0.partition, format: .number).monospacedDigit() }
                    .width(Metrics.partitionWidth)
                TableColumn("Current") { row in
                    if let committed = row.currentCommitted { Text(committed, format: .number).monospacedDigit() } else {
                        Text("none").foregroundStyle(.secondary)
                    }
                }
                .width(min: Metrics.numberWidth)
                TableColumn("New") { Text($0.newOffset, format: .number).monospacedDigit() }
                    .width(min: Metrics.numberWidth)
                TableColumn("Lag") { row in
                    Text("\(row.lagBefore.formatted()) → \(row.lagAfter.formatted())").monospacedDigit()
                }
                .width(min: Metrics.numberWidth)
                TableColumn("Result") { row in
                    let status = OffsetRowStatus(row)
                    Label(status.label, systemImage: status.symbolName)
                }
                .width(min: Metrics.statusWidth)
            }
            .tableStyle(.inset(alternatesRowBackgrounds: true))
            Text("Only partitions this group has committed to are listed.").font(.caption).foregroundStyle(.secondary)
        }
    }

    private func totalsText(_ preview: OffsetResetPreview) -> String {
        let count = preview.rows.count
        let partitions = count == 1 ? String(localized: "1 partition") : String(localized: "\(count) partitions")
        return String(localized: "Lag \(preview.totalLagBefore.formatted()) → \(preview.totalLagAfter.formatted()) across \(partitions)")
    }

    @ViewBuilder private var outcome: some View {
        if let error = model.applyError {
            ErrorBanner(message: error, onDismiss: nil)
        }
        if let result = model.result {
            if let note = model.successNote {
                Label(note, systemImage: "checkmark.circle.fill").foregroundStyle(.green).accessibilityElement(children: .combine)
            }
            if !result.failures.isEmpty {
                VStack(alignment: .leading, spacing: 2) {
                    Label("\(result.applied) partitions reset, \(result.failures.count) failed", systemImage: "exclamationmark.triangle.fill")
                        .foregroundStyle(.orange)
                    ForEach(result.failures.prefix(5), id: \.self) { failure in
                        Text("\(failure.topic) #\(failure.partition): \(failure.message)").font(.caption)
                    }
                    if result.failures.count > 5 { Text("and \(result.failures.count - 5) more").font(.caption).foregroundStyle(.secondary) }
                }
            }
        }
    }

    private var buttons: some View {
        HStack {
            if model.isApplying { ProgressView().controlSize(.small).accessibilityLabel(Text("Applying")) }
            Spacer()
            Button(model.successNote == nil ? "Cancel" : "Done") { dismiss() }
                .keyboardShortcut(.cancelAction)
            Button("Apply…") { model.requestApply() }
                .keyboardShortcut(.defaultAction)
                .disabled(model.applyDisabledReason != nil)
                .help(Text(model.applyDisabledReason ?? String(localized: "Review and confirm the reset")))
        }
    }
}

/// What the group detail's header needs to offer a reset.
struct ResetControl {
    let gate: WriteGate
    let open: (OffsetTargetKind) -> Void
}

/// The one-click presets, shared by the detail header and the list's context menu.
struct ResetPresetButtons: View {
    let open: (OffsetTargetKind) -> Void

    var body: some View {
        Button("Skip to Latest (clear lag)") { open(.latest) }
        Button("Rewind to Earliest") { open(.earliest) }
        Button("To Time…") { open(.time) }
        Button("To Offset…") { open(.offset) }
    }
}
