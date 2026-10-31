import SwiftUI
import KafkaManagerCore

private enum Metrics {
    static let spacing: Double = 8
    static let padding: Double = 12
    static let tableInset: Double = 12
    static let tableGap: Double = 6
    static let idWidth: ClosedRange<Double> = 30...60
    static let leaderWidth: ClosedRange<Double> = 40...70
    static let statusMin: Double = 100
    static let flagMin: Double = 100
}

/// Header plus Partitions / Config tabs for one topic.
struct TopicDetailView: View {
    @Bindable var model: TopicDetailViewModel
    /// nil hides the delete button; otherwise it is shown, disabled with a reason when writes are not allowed.
    let delete: (gate: WriteGate, isBusy: Bool, action: () -> Void)?

    @State private var refreshCount = 0


    init(model: TopicDetailViewModel, delete: (gate: WriteGate, isBusy: Bool, action: () -> Void)? = nil) {
        self.model = model
        self.delete = delete
    }

    var body: some View {
        VStack(spacing: 0) {
            header
            Divider()
            StateView(
                state: model.state,
                isEmpty: { _ in false },
                loadingTitle: String(localized: "Loading topic…"),
                failureTitle: String(localized: "Couldn't load the topic"),
                emptyTitle: String(localized: "No details"),
                emptySymbol: "tray",
                retry: { refreshCount += 1 },
                isRefreshing: model.isRefreshing
            ) { detail in
                VStack(spacing: 0) {
                    Picker("Details", selection: $model.tab) {
                        Text("Partitions").tag(TopicDetailViewModel.Tab.partitions)
                        Text("Config").tag(TopicDetailViewModel.Tab.config)
                        Text("Messages").tag(TopicDetailViewModel.Tab.messages)
                    }
                    .pickerStyle(.segmented)
                    .labelsHidden()
                    .padding(Metrics.padding)
                    switch model.tab {
                    case .partitions: PartitionsTable(partitions: detail.partitions)
                    case .config: ConfigTable(entries: detail.configEntries)
                    case .messages: MessagesView(model: model.messages)
                    }
                }
            }
        }
        // Loads once per topic; a changed count is an explicit refresh.
        .task(id: refreshCount) {
            if refreshCount == 0 { await model.loadIfNeeded() } else { await model.refresh() }
        }
    }

    private var header: some View {
        HStack(alignment: .firstTextBaseline, spacing: Metrics.spacing) {
            VStack(alignment: .leading, spacing: 2) {
                Text(model.topicName).font(.title2.weight(.semibold)).textSelection(.enabled).lineLimit(2)
                if let detail = model.state.value {
                    Text(summary(detail)).font(.callout).foregroundStyle(.secondary)
                }
            }
            Spacer()
            Button {
                refreshCount += 1
            } label: {
                Label("Refresh", systemImage: "arrow.clockwise").labelStyle(.iconOnly)
            }
            .help(Text("Refresh this topic"))
            .disabled(model.isRefreshing || model.state.isLoading)
            if let delete {
                Button(role: .destructive, action: delete.action) {
                    Label("Delete Topic…", systemImage: "trash")
                }
                .help(Text(delete.gate.disabledReason ?? String(localized: "Delete this topic")))
                .disabled(!delete.gate.allowsWrites || delete.isBusy)
            }
        }
        .padding(Metrics.padding)
    }

    private func summary(_ detail: TopicDetail) -> String {
        let partitions = detail.partitions.count == 1 ? String(localized: "1 partition") : String(localized: "\(detail.partitions.count) partitions")
        let replication = String(localized: "replication factor \(detail.partitions.first?.replicas.count ?? 0)")
        return "\(partitions) · \(replication)"
    }
}

private struct PartitionsTable: View {
    let rows: [PartitionRow]

    init(partitions: [PartitionInfo]) {
        rows = PartitionPresentation.rows(partitions)
    }

    var body: some View {
        VStack(spacing: 0) {
            if let summary = PartitionPresentation.summary(rows) {
                Label(summary, systemImage: PartitionFlag.underReplicated.symbolName)
                    .foregroundStyle(.orange)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(.horizontal, Metrics.tableInset)
                    .padding(.bottom, Metrics.tableGap)
            }
            Table(rows) {
                TableColumn("ID") { Text($0.id, format: .number.grouping(.never)).monospacedDigit() }.width(min: Metrics.idWidth.lowerBound, max: Metrics.idWidth.upperBound)
                TableColumn("Leader") { Text($0.leaderText).monospacedDigit() }.width(min: Metrics.leaderWidth.lowerBound, max: Metrics.leaderWidth.upperBound)
                TableColumn("Replicas") { Text($0.replicasText).monospacedDigit() }
                TableColumn("ISR") { Text($0.isrText).monospacedDigit() }
                TableColumn("Start") { Text($0.startOffset, format: .number).monospacedDigit() }
                TableColumn("End") { Text($0.endOffset, format: .number).monospacedDigit() }
                TableColumn("Messages") { Text($0.messageCount, format: .number).monospacedDigit() }
                TableColumn("Status") { row in
                    if row.isHealthy {
                        Label("OK", systemImage: "checkmark.circle").foregroundStyle(.secondary)
                    } else {
                        // Icon and words: color is not the only signal.
                        Label(row.flagsText, systemImage: row.flags[0].symbolName).foregroundStyle(.orange)
                    }
                }
                .width(min: Metrics.statusMin)
            }
            .tableStyle(.inset(alternatesRowBackgrounds: true))
        }
    }
}

private struct ConfigTable: View {
    let entries: [ConfigEntry]
    @State private var onlyNonDefault = false

    var body: some View {
        let rows = ConfigPresentation.rows(entries, onlyNonDefault: onlyNonDefault)
        VStack(spacing: 0) {
            HStack {
                Toggle("Only non-default", isOn: $onlyNonDefault).toggleStyle(.checkbox).controlSize(.small)
                Spacer()
                Text(rows.count == 1 ? String(localized: "1 setting") : String(localized: "\(rows.count) settings"))
                    .font(.caption).foregroundStyle(.secondary).monospacedDigit()
            }
            .padding(.horizontal, Metrics.tableInset)
            .padding(.bottom, Metrics.tableGap)
            if rows.isEmpty {
                ContentUnavailableView("No settings", systemImage: "slider.horizontal.3")
            } else {
                Table(rows) {
                    TableColumn("Setting") { Text($0.name).textSelection(.enabled) }
                    TableColumn("Value") { row in
                        if row.isSensitive {
                            Label(row.valueText, systemImage: "lock.fill").foregroundStyle(.secondary)
                        } else {
                            Text(row.valueText).textSelection(.enabled).lineLimit(1).truncationMode(.middle)
                        }
                    }
                    TableColumn("") { row in
                        if row.isNonDefault {
                            Label("Non-default", systemImage: "slider.horizontal.3").foregroundStyle(.blue)
                        }
                    }
                    .width(min: Metrics.flagMin)
                }
                .tableStyle(.inset(alternatesRowBackgrounds: true))
            }
        }
    }
}
