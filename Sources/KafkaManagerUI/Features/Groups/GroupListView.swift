import SwiftUI
import KafkaManagerCore

/// Searchable, sortable table of a cluster's consumer groups. Loading, refreshing and filtering live in the view model.
public struct GroupListView: View {
    @Bindable var model: GroupListViewModel
    /// ⌘R from the menu; changes after the view appeared reload the list.
    let refreshToken: Int

    @State private var refreshCount = 0
    @FocusState private var searchFocused: Bool

    private enum Metrics {
        static let spacing: Double = 8
        static let padding: Double = 10
        static let countMinWidth: Double = 90
        static let membersMin: Double = 70
        static let membersMax: Double = 100
        static let stateMin: Double = 150
    }

    public init(model: GroupListViewModel, refreshToken: Int) {
        self.model = model
        self.refreshToken = refreshToken
    }

    public var body: some View {
        VStack(spacing: 0) {
            header
            Divider()
            StateView(
                state: model.state,
                isEmpty: { $0.isEmpty },
                loadingTitle: String(localized: "Loading consumer groups…"),
                failureTitle: String(localized: "Couldn't load consumer groups"),
                emptyTitle: String(localized: "No consumer groups"),
                emptyMessage: String(localized: "Nothing has joined a group on this cluster yet."),
                emptySymbol: "person.3",
                retry: { refreshCount += 1 },
                isRefreshing: model.isRefreshing
            ) { _ in
                if model.hasNoMatches {
                    ContentUnavailableView.search(text: model.searchText)
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
        .focusedSceneValue(\.findAction, findAction)
    }

    private var findAction: (() -> Void)? { { searchFocused = true } }

    private var header: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            HStack(spacing: Metrics.spacing) {
                TextField("Search groups", text: $model.searchText)
                    .textFieldStyle(.roundedBorder)
                    .focused($searchFocused)
                    .accessibilityLabel(Text("Search groups"))
                Button {
                    refreshCount += 1
                } label: {
                    Label("Refresh", systemImage: "arrow.clockwise").labelStyle(.iconOnly)
                }
                .help(Text("Refresh consumer groups"))
                .disabled(model.isRefreshing || model.state.isLoading)
            }
            HStack {
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

    private var table: some View {
        Table(model.visibleGroups, selection: $model.selection, sortOrder: sortOrder) {
            TableColumn("Group ID", value: \.id) { group in
                Text(group.id).lineLimit(1).truncationMode(.middle)
            }
            TableColumn("State", value: \.state) { group in
                GroupStateLabel(state: group.groupState)
            }
            .width(min: Metrics.stateMin)
            TableColumn("Members", value: \.memberCount) { group in
                Text(group.memberCount, format: .number).monospacedDigit()
            }
            .width(min: Metrics.membersMin, max: Metrics.membersMax)
        }
        .tableStyle(.inset(alternatesRowBackgrounds: true))
    }

    /// The view model owns the sort; the table's header just reads and writes it.
    private var sortOrder: Binding<[KeyPathComparator<GroupSummary>]> {
        Binding(
            get: {
                let order: SortOrder = model.sortAscending ? .forward : .reverse
                switch model.sortKey {
                case .id: return [KeyPathComparator(\GroupSummary.id, order: order)]
                case .state: return [KeyPathComparator(\GroupSummary.state, order: order)]
                case .members: return [KeyPathComparator(\GroupSummary.memberCount, order: order)]
                }
            },
            set: { comparators in
                guard let first = comparators.first else { return }
                switch first.keyPath {
                case \GroupSummary.state: model.sortKey = .state
                case \GroupSummary.memberCount: model.sortKey = .members
                default: model.sortKey = .id
                }
                model.sortAscending = first.order == .forward
            })
    }
}

/// Symbol and text together; the tint only reinforces them.
struct GroupStateLabel: View {
    let state: GroupState

    var body: some View {
        let presentation = GroupStatePresentation(state)
        Label(presentation.label, systemImage: presentation.symbolName)
            .foregroundStyle(tint(presentation.tone))
            .lineLimit(1)
    }

    private func tint(_ tone: GroupStatePresentation.Tone) -> Color {
        switch tone {
        case .good: .green
        case .busy: .orange
        case .bad: .red
        case .idle, .unknown: .secondary
        }
    }
}
