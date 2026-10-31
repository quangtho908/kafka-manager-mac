import SwiftUI
import KafkaManagerCore

/// Header plus Lag / Members tabs for one consumer group.
struct GroupDetailView: View {
    @Bindable var model: GroupDetailViewModel
    /// nil where resetting is not wired (previews); the list model owns the reset sheet.
    let reset: ResetControl?

    @State private var refreshCount = 0
    /// Topics the user folded; everything else is open.
    @State private var collapsed: Set<String> = []

    private enum Metrics {
        static let spacing: Double = 8
        static let padding: Double = 12
        static let tableInset: Double = 12
        static let partitionWidth: ClosedRange<Double> = 90...160
        static let committedWidth: ClosedRange<Double> = 90...140
        static let endWidth: ClosedRange<Double> = 80...130
        static let lagMin: Double = 150
        static let memberMin: Double = 100
        static let intervalWidth: Double = 70
        static let hostMin: Double = 100
    }

    init(model: GroupDetailViewModel, reset: ResetControl? = nil) {
        self.model = model
        self.reset = reset
    }

    var body: some View {
        VStack(spacing: 0) {
            header
            Divider()
            StateView(
                state: model.state,
                isEmpty: { _ in false },
                loadingTitle: String(localized: "Loading group…"),
                failureTitle: String(localized: "Couldn't load the group"),
                emptyTitle: String(localized: "No details"),
                emptySymbol: "tray",
                retry: { refreshCount += 1 },
                isRefreshing: model.isRefreshing
            ) { _ in
                VStack(spacing: 0) {
                    notices
                    Picker("Details", selection: $model.tab) {
                        Text("Lag").tag(GroupDetailViewModel.Tab.lag)
                        Text("Members").tag(GroupDetailViewModel.Tab.members)
                    }
                    .pickerStyle(.segmented)
                    .labelsHidden()
                    .padding(Metrics.padding)
                    switch model.tab {
                    case .lag: lagTable
                    case .members: membersTable
                    }
                }
            }
        }
        .topAlignedColumn()
        // Loads once per group; a changed count is an explicit refresh.
        .task(id: refreshCount) {
            if refreshCount == 0 { await model.loadIfNeeded() } else { await model.refresh() }
        }
        // Exists only while the toggle is on and this view is on screen.
        .task(id: model.autoRefreshKey) { await model.autoRefresh() }
    }

    // MARK: Header

    private var header: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            HStack(alignment: .firstTextBaseline, spacing: Metrics.spacing) {
                Text(model.groupId).font(.title2.weight(.semibold)).textSelection(.enabled).lineLimit(2)
                if let info = model.header { GroupStateLabel(state: info.state).font(.callout) }
                Spacer()
                autoRefreshControls
                Button {
                    refreshCount += 1
                } label: {
                    Label("Refresh", systemImage: "arrow.clockwise").labelStyle(.iconOnly)
                }
                .help(Text("Refresh this group"))
                .disabled(model.isRefreshing || model.state.isLoading)
                if let reset { resetMenu(reset) }
            }
            if let info = model.header { facts(info) }
        }
        .padding(Metrics.padding)
    }

    /// Icon and words; disabled with the reason as tooltip for a read-only cluster or a group that still has members.
    private func resetMenu(_ reset: ResetControl) -> some View {
        let reason = reset.gate.disabledReason
            ?? model.header.flatMap { OffsetResetRule.refusal(for: $0.state) }
            ?? (model.header == nil ? String(localized: "Loading the group…") : nil)
        return Menu {
            ResetPresetButtons(open: reset.open)
        } label: {
            Label("Reset Offsets…", systemImage: "arrow.counterclockwise")
        }
        .fixedSize()
        .help(Text(reason ?? String(localized: "Move the committed offsets of this group")))
        .disabled(reason != nil)
    }

    private var autoRefreshControls: some View {
        HStack(spacing: Metrics.spacing / 2) {
            Toggle("Auto-refresh", isOn: $model.autoRefreshOn)
                .toggleStyle(.checkbox)
                .controlSize(.small)
            Picker("Interval", selection: $model.autoRefreshInterval) {
                ForEach(AutoRefreshInterval.allCases) { Text($0.title).tag($0) }
            }
            .labelsHidden()
            .controlSize(.small)
            .frame(width: Metrics.intervalWidth)
            .disabled(!model.autoRefreshOn)
            .accessibilityLabel(Text("Auto-refresh interval"))
        }
    }

    private func facts(_ info: GroupHeaderInfo) -> some View {
        let severity = LagSeverityPresentation(info.severity)
        return HStack(spacing: Metrics.padding) {
            Label {
                Text("Total lag \(info.totalLagShort)").monospacedDigit()
            } icon: {
                Image(systemName: severity.symbolName)
            }
            .foregroundStyle(LagTint.color(severity.tone))
            .help(Text("\(info.totalLagExact) messages behind · \(severity.label)"))
            Text("\(info.memberCount) members")
            Text("Coordinator: \(info.coordinatorText)")
            Text("Assignor: \(info.assignorText)")
        }
        .font(.callout)
        .foregroundStyle(.secondary)
        .lineLimit(1)
    }

    @ViewBuilder private var notices: some View {
        if let info = model.header, let warning = info.warningText {
            Label(warning, systemImage: "exclamationmark.triangle")
                .font(.callout)
                .foregroundStyle(.orange)
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(.horizontal, Metrics.padding)
                .padding(.top, Metrics.spacing)
        }
    }

    // MARK: Lag

    private var lagTable: some View {
        Table(of: LagRow.self) {
            TableColumn("Partition") { row in
                if row.kind == .topic {
                    Text(row.topic).fontWeight(.semibold).lineLimit(1).truncationMode(.middle)
                } else {
                    Text(row.partition ?? 0, format: .number.grouping(.never)).monospacedDigit()
                }
            }
            .width(min: Metrics.partitionWidth.lowerBound, ideal: Metrics.partitionWidth.upperBound)
            TableColumn("Committed") { row in committedCell(row) }
                .width(min: Metrics.committedWidth.lowerBound, max: Metrics.committedWidth.upperBound)
            TableColumn("End offset") { row in
                if let end = row.endOffset { Text(end, format: .number.grouping(.never)).monospacedDigit() }
            }
            .width(min: Metrics.endWidth.lowerBound, max: Metrics.endWidth.upperBound)
            TableColumn("Lag") { row in lagCell(row) }
                .width(min: Metrics.lagMin)
            TableColumn("Member") { row in
                Text(row.memberText).lineLimit(1).help(Text(row.memberHelp ?? ""))
            }
            .width(min: Metrics.memberMin)
        } rows: {
            ForEach(model.lagSections) { section in
                DisclosureTableRow(section.header, isExpanded: expansion(of: section.topic)) {
                    ForEach(section.partitions) { TableRow($0) }
                }
            }
        }
        .tableStyle(.inset(alternatesRowBackgrounds: true))
    }

    private func expansion(of topic: String) -> Binding<Bool> {
        Binding(
            get: { !collapsed.contains(topic) },
            set: { open in
                if open { collapsed.remove(topic) } else { collapsed.insert(topic) }
            })
    }

    @ViewBuilder private func committedCell(_ row: LagRow) -> some View {
        if row.kind == .partition {
            if row.hasNoOffset {
                Label(row.committedText, systemImage: "questionmark.circle")
                    .foregroundStyle(.secondary)
                    .help(Text(row.note ?? ""))
            } else if row.isAheadOfEnd {
                Label(row.committedText, systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.orange)
                    .help(Text(row.note ?? ""))
            } else {
                Text(row.committedText).monospacedDigit()
            }
        } else if row.hasNoOffset || row.isAheadOfEnd {
            Image(systemName: "exclamationmark.triangle")
                .foregroundStyle(.orange)
                .accessibilityLabel(Text("Some partitions need attention"))
        }
    }

    private func lagCell(_ row: LagRow) -> some View {
        let severity = LagSeverityPresentation(row.severity)
        return HStack(spacing: Metrics.spacing / 2) {
            Image(systemName: severity.symbolName)
            Text(GroupLag.shortText(row.lag)).monospacedDigit().fontWeight(row.kind == .topic ? .semibold : .regular)
            Text(severity.label).font(.caption)
        }
        .foregroundStyle(LagTint.color(severity.tone))
        .help(Text("\(GroupLag.exactText(row.lag)) messages"))
    }

    // MARK: Members

    @ViewBuilder private var membersTable: some View {
        let rows = model.memberRows
        if rows.isEmpty {
            ContentUnavailableView(
                "No members", systemImage: "person.slash",
                description: Text("Nothing is consuming with this group right now."))
        } else {
            Table(rows) {
                TableColumn("Client ID") { Text($0.clientId).lineLimit(1).truncationMode(.middle) }
                TableColumn("Member ID") { Text($0.id).lineLimit(1).truncationMode(.middle).textSelection(.enabled) }
                TableColumn("Host") { Text($0.host).lineLimit(1) }
                    .width(min: Metrics.hostMin)
                TableColumn("Partitions") { row in
                    Text(row.partitionCount, format: .number).monospacedDigit()
                }
                .width(min: Metrics.endWidth.lowerBound, max: Metrics.endWidth.upperBound)
                TableColumn("Assigned") { row in
                    Text(row.partitionsText).lineLimit(1).help(Text(row.partitionsFull))
                }
            }
            .tableStyle(.inset(alternatesRowBackgrounds: true))
        }
    }
}

enum LagTint {
    static func color(_ tone: LagSeverityPresentation.Tone) -> Color {
        switch tone {
        case .good: .green
        case .caution: .yellow
        case .warning: .orange
        case .critical: .red
        }
    }
}
