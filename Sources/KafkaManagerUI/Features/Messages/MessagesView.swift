import SwiftUI
import KafkaManagerCore

/// Query bar, filter and results table for one topic's messages. Selecting a row only records the selection.
struct MessagesView: View {
    @Bindable var model: MessagesViewModel

    /// Bumped by Fetch; `.task(id:)` runs the fetch and cancels it when the view goes away or the token changes.
    @State private var runToken = 0
    @State private var sortOrder: [KeyPathComparator<MessageRow>] = [KeyPathComparator(\.message.timestamp, order: .reverse)]
    @FocusState private var filterFocused: Bool
    @Environment(\.clipboard) private var clipboard

    private enum Metrics {
        static let padding: Double = 12
        static let spacing: Double = 8
        static let numberWidth: Double = 70
        static let offsetWidth: Double = 110
        static let pickerWidth: Double = 120
        static let partitionWidth: Double = 56
        static let offsetColumn: ClosedRange<Double> = 70...120
        static let timestampColumn: ClosedRange<Double> = 150...200
        static let sizeColumn: ClosedRange<Double> = 60...100
        static let tableMinHeight: Double = 120
        static let keyMin: Double = 80
        static let valueMin: Double = 160
    }

    var body: some View {
        VStack(spacing: 0) {
            queryBar
            if !model.queryNotes.isEmpty { notes }
            Divider()
            filterBar
            if case .failed(let message, _) = model.status {
                ErrorBanner(message: message, onRetry: { runToken += 1 })
            }
            resultsAndDetail
            statusBar
        }
        .topAlignedColumn()
        .task(id: runToken) {
            guard runToken > 0 else { return }
            await model.fetch()
        }
        .onChange(of: sortOrder) { _, order in applySort(order) }
        .focusedSceneValue(\.findAction, { filterFocused = true })
    }

    // MARK: Query bar

    private var queryBar: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            HStack(spacing: Metrics.spacing) {
                Picker("Partition", selection: $model.partition) {
                    Text("All partitions").tag(Int?.none)
                    ForEach(model.partitionIDs, id: \.self) { id in
                        Text("Partition \(id)").tag(Optional(id))
                    }
                }
                .frame(maxWidth: Metrics.pickerWidth * 1.5)
                Picker("Start", selection: $model.startKind) {
                    Text("Latest").tag(MessageStartKind.latest)
                    Text("Earliest").tag(MessageStartKind.earliest)
                    Text("Offset").tag(MessageStartKind.offset)
                    Text("Timestamp").tag(MessageStartKind.timestamp)
                }
                .frame(maxWidth: Metrics.pickerWidth * 1.5)
                startInput
            }
            HStack(spacing: Metrics.spacing) {
                TextField("Limit", value: $model.limitInput, format: .number.grouping(.never))
                    .textFieldStyle(.roundedBorder)
                    .frame(width: Metrics.numberWidth)
                    .monospacedDigit()
                    .accessibilityLabel(Text("Limit"))
                Stepper("Limit", value: $model.limitInput, in: 1...MessagesViewModel.maxLimit, step: 100)
                    .labelsHidden()
                Text("messages max")
                    .font(.callout)
                    .foregroundStyle(.secondary)
                Spacer()
                if model.isFetching {
                    ProgressView().controlSize(.small).accessibilityLabel(Text("Fetching"))
                    Button { model.stop() } label: { Label("Stop", systemImage: "stop.fill") }
                } else {
                    Button { runToken += 1 } label: { Label("Fetch", systemImage: "arrow.down.circle") }
                        .keyboardShortcut(.return, modifiers: .command)
                        .disabled(!model.canFetch)
                }
            }
            if let issue = model.queryIssues.first {
                Label(issue.message, systemImage: "exclamationmark.triangle.fill")
                    .font(.callout)
                    .foregroundStyle(.orange)
            }
        }
        .padding(Metrics.padding)
    }

    @ViewBuilder private var startInput: some View {
        switch model.startKind {
        case .latest:
            TextField("Count", value: $model.latestCount, format: .number.grouping(.never))
                .textFieldStyle(.roundedBorder)
                .frame(width: Metrics.numberWidth)
                .monospacedDigit()
                .accessibilityLabel(Text("Number of recent messages"))
        case .offset:
            TextField("Offset", value: $model.startOffset, format: .number.grouping(.never))
                .textFieldStyle(.roundedBorder)
                .frame(width: Metrics.offsetWidth)
                .monospacedDigit()
                .accessibilityLabel(Text("Start offset"))
        case .timestamp:
            DatePicker("Since", selection: $model.startDate)
                .labelsHidden()
        case .earliest:
            EmptyView()
        }
    }

    private var notes: some View {
        VStack(alignment: .leading, spacing: 2) {
            ForEach(model.queryNotes, id: \.self) { note in
                Label(note, systemImage: "info.circle").font(.callout).foregroundStyle(.secondary)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.horizontal, Metrics.padding)
        .padding(.bottom, Metrics.spacing)
    }

    // MARK: Filter

    private var filterBar: some View {
        TextField("Filter fetched messages", text: $model.searchText)
            .textFieldStyle(.roundedBorder)
            .focused($filterFocused)
            .accessibilityLabel(Text("Filter fetched messages"))
            .padding(Metrics.padding)
    }

    // MARK: Results

    /// The table, with the selected message's detail in a resizable split below it.
    @ViewBuilder private var resultsAndDetail: some View {
        if let row = model.selectedRow {
            VSplitView {
                results.frame(minHeight: Metrics.tableMinHeight)
                MessageDetailView(topic: model.topicName, message: row.message, clipboard: clipboard)
                    .id(row.id)
            }
        } else {
            results
        }
    }

    @ViewBuilder private var results: some View {
        if model.rows.isEmpty {
            emptyState
        } else if model.hasNoMatches {
            ContentUnavailableView.search(text: model.searchText)
        } else {
            table
        }
    }

    @ViewBuilder private var emptyState: some View {
        switch model.status {
        case .idle:
            ContentUnavailableView(
                "No messages loaded", systemImage: "tray",
                description: Text("Choose where to start and press Fetch."))
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        case .fetching:
            ProgressView("Fetching…").frame(maxWidth: .infinity, maxHeight: .infinity)
        case .finished:
            ContentUnavailableView(
                "No messages", systemImage: "tray",
                description: Text("Nothing was found for this query."))
        case .failed:
            Spacer()
        }
    }

    private var table: some View {
        Table(model.visibleRows, selection: $model.selection, sortOrder: $sortOrder) {
            TableColumn("Partition", value: \.message.partition) { row in
                Text(row.message.partition, format: .number.grouping(.never)).monospacedDigit()
            }
            .width(min: Metrics.partitionWidth, max: Metrics.partitionWidth * 1.6)
            TableColumn("Offset", value: \.message.offset) { row in
                Text(row.message.offset, format: .number.grouping(.never)).monospacedDigit()
            }
            .width(min: Metrics.offsetColumn.lowerBound, max: Metrics.offsetColumn.upperBound)
            TableColumn("Timestamp", value: \.message.timestamp) { row in
                Text(row.message.timestamp, format: .dateTime.year().month().day().hour().minute().second())
                    .monospacedDigit()
            }
            .width(min: Metrics.timestampColumn.lowerBound, max: Metrics.timestampColumn.upperBound)
            TableColumn("Key") { row in
                Text(row.keyPreview.display).lineLimit(1).foregroundStyle(row.message.key == nil ? .secondary : .primary)
            }
            .width(min: Metrics.keyMin)
            TableColumn("Value") { row in
                if row.isTombstone {
                    Label("null (tombstone)", systemImage: "trash.slash").lineLimit(1).foregroundStyle(.secondary)
                } else {
                    Text(row.valuePreview.display).lineLimit(1)
                }
            }
            .width(min: Metrics.valueMin)
            TableColumn("Size", value: \.size) { row in
                Text(ByteCountFormatStyle().format(Int64(row.size))).monospacedDigit()
            }
            .width(min: Metrics.sizeColumn.lowerBound, max: Metrics.sizeColumn.upperBound)
        }
        .tableStyle(.inset(alternatesRowBackgrounds: true))
        .contextMenu(forSelectionType: MessageID.self) { ids in
            if let id = ids.first {
                Button("Copy Raw Key") { model.copyRaw(.key, of: id, to: clipboard) }
                    .disabled(!model.canCopyRaw(.key, of: id))
                Button("Copy Raw Value") { model.copyRaw(.value, of: id, to: clipboard) }
                    .disabled(!model.canCopyRaw(.value, of: id))
            }
        }
    }

    private var statusBar: some View {
        HStack {
            Text(model.statusText).font(.caption).foregroundStyle(.secondary).monospacedDigit()
            if let note = model.limitNote {
                Text(note).font(.caption).foregroundStyle(.orange)
            }
            Spacer()
            if !model.searchText.isEmpty, !model.rows.isEmpty {
                Text("\(model.visibleRows.count) of \(model.rows.count) shown")
                    .font(.caption).foregroundStyle(.secondary).monospacedDigit()
            }
        }
        .padding(.horizontal, Metrics.padding)
        .padding(.vertical, Metrics.spacing / 2)
    }

    private func applySort(_ order: [KeyPathComparator<MessageRow>]) {
        guard let first = order.first else { return }
        let key: MessageSortKey
        switch first.keyPath {
        case \MessageRow.message.partition: key = .partition
        case \MessageRow.message.offset: key = .offset
        case \MessageRow.size: key = .size
        default: key = .timestamp
        }
        model.sortKey = key
        model.sortAscending = first.order == .forward
    }
}
