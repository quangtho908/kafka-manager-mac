import SwiftUI
import KafkaManagerCore

/// Searchable, sortable table of a cluster's topics. Loading, refreshing and filtering live in the view model.
public struct TopicListView: View {
    @Bindable var model: TopicListViewModel
    /// ⌘R from the menu; changes after the view appeared reload the list.
    let refreshToken: Int

    @State private var refreshCount = 0
    @FocusState private var searchFocused: Bool

    private enum Metrics {
        static let spacing: Double = 8
        static let padding: Double = 10
        static let countMinWidth: Double = 90
        static let numberWidth: Double = 80
        static let replicationMin: Double = 80
        static let replicationMax: Double = 120
    }

    public init(model: TopicListViewModel, refreshToken: Int) {
        self.model = model
        self.refreshToken = refreshToken
    }

    public var body: some View {
        VStack(spacing: 0) {
            header
            if let notice = model.removedTopicNotice {
                HStack {
                    Label(notice, systemImage: "trash.slash").font(.callout)
                    Spacer()
                    Button("Dismiss") { model.dismissRemovedTopicNotice() }.controlSize(.small)
                }
                .padding(Metrics.padding)
                .background(Color.orange.opacity(0.18))
            }
            Divider()
            StateView(
                state: model.state,
                isEmpty: { $0.isEmpty },
                loadingTitle: String(localized: "Loading topics…"),
                failureTitle: String(localized: "Couldn't load topics"),
                emptyTitle: String(localized: "No topics"),
                emptyMessage: String(localized: "This cluster has no topics yet."),
                emptySymbol: "tray",
                retry: { refreshCount += 1 },
                isRefreshing: model.isRefreshing
            ) { _ in
                if model.hasNoMatches {
                    noMatches
                } else {
                    table
                }
            }
        }
        // First run loads when needed (and reloads a list older than 30 s); later runs are explicit refreshes.
        .task(id: refreshCount) {
            if refreshCount == 0 { await model.loadIfNeeded() } else { await model.refresh() }
        }
        .onChange(of: refreshToken) { refreshCount += 1 }
        .focusedSceneValue(\.findAction, focusSearch)
    }

    private func focusSearch() { searchFocused = true }

    private var header: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            HStack(spacing: Metrics.spacing) {
                TextField("Search topics", text: $model.searchText)
                    .textFieldStyle(.roundedBorder)
                    .focused($searchFocused)
                    .accessibilityLabel(Text("Search topics"))
                Button {
                    refreshCount += 1
                } label: {
                    Label("Refresh", systemImage: "arrow.clockwise").labelStyle(.iconOnly)
                }
                .help(Text("Refresh topics"))
                .disabled(model.isRefreshing || model.state.isLoading)
            }
            HStack {
                Toggle("Show internal", isOn: $model.showInternal)
                    .toggleStyle(.checkbox)
                    .controlSize(.small)
                Spacer()
                if model.state.value != nil {
                    Text(model.countText)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .monospacedDigit()
                }
            }
        }
        .padding(Metrics.padding)
    }

    @ViewBuilder private var noMatches: some View {
        if model.searchText.isEmpty {
            ContentUnavailableView(
                "No topics to show", systemImage: "eye.slash",
                description: Text("Only internal topics exist. Turn on Show internal to see them."))
        } else {
            ContentUnavailableView.search(text: model.searchText)
        }
    }

    private var table: some View {
        Table(model.visibleTopics, selection: $model.selection, sortOrder: sortOrder) {
            TableColumn("Name", value: \.name) { topic in
                Text(topic.name).lineLimit(1).truncationMode(.middle)
            }
            TableColumn("Partitions", value: \.partitionCount) { topic in
                Text(topic.partitionCount, format: .number).monospacedDigit()
            }
            .width(min: Metrics.numberWidth)
            TableColumn("Replication", value: \.replicationFactor) { topic in
                Text(topic.replicationFactor, format: .number).monospacedDigit()
            }
            .width(min: Metrics.replicationMin, max: Metrics.replicationMax)
        }
        .tableStyle(.inset(alternatesRowBackgrounds: true))
    }

    /// The view model owns the sort; the table's header just reads and writes it.
    private var sortOrder: Binding<[KeyPathComparator<TopicSummary>]> {
        Binding(
            get: {
                let order: SortOrder = model.sortAscending ? .forward : .reverse
                switch model.sortKey {
                case .name: return [KeyPathComparator(\TopicSummary.name, order: order)]
                case .partitions: return [KeyPathComparator(\TopicSummary.partitionCount, order: order)]
                case .replicationFactor: return [KeyPathComparator(\TopicSummary.replicationFactor, order: order)]
                }
            },
            set: { comparators in
                guard let first = comparators.first else { return }
                switch first.keyPath {
                case \TopicSummary.partitionCount: model.sortKey = .partitions
                case \TopicSummary.replicationFactor: model.sortKey = .replicationFactor
                default: model.sortKey = .name
                }
                model.sortAscending = first.order == .forward
            })
    }
}
