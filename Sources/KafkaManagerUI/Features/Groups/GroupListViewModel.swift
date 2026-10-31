import Foundation
import Observation
import KafkaManagerCore

public enum GroupSortKey: Sendable, Hashable, CaseIterable {
    case id, state, members
}

/// How a state is shown: text and symbol always together, never color alone.
public struct GroupStatePresentation: Sendable, Equatable {
    public enum Tone: Sendable, Equatable { case good, busy, idle, bad, unknown }

    public let label: String
    public let symbolName: String
    public let tone: Tone
    /// Orders states by how healthy they are: Stable, rebalancing, Empty, Dead, anything else.
    let rank: Int

    public init(_ state: GroupState) {
        switch state {
        case .stable:
            self.init(label: String(localized: "Stable"), symbolName: "checkmark.circle", tone: .good, rank: 0)
        case .preparingRebalance:
            self.init(label: String(localized: "Rebalancing (preparing)"), symbolName: "arrow.triangle.2.circlepath", tone: .busy, rank: 1)
        case .completingRebalance:
            self.init(label: String(localized: "Rebalancing (completing)"), symbolName: "arrow.triangle.2.circlepath", tone: .busy, rank: 2)
        case .empty:
            self.init(label: String(localized: "Empty"), symbolName: "circle.dashed", tone: .idle, rank: 3)
        case .dead:
            self.init(label: String(localized: "Dead"), symbolName: "xmark.octagon", tone: .bad, rank: 4)
        case .other(let name):
            let shown = name.trimmingCharacters(in: .whitespaces)
            self.init(label: shown.isEmpty ? String(localized: "Unknown") : shown, symbolName: "questionmark.circle", tone: .unknown, rank: 5)
        }
    }

    private init(label: String, symbolName: String, tone: Tone, rank: Int) {
        self.label = label
        self.symbolName = symbolName
        self.tone = tone
        self.rank = rank
    }
}

/// The consumer groups of one cluster: one `listGroups` call per load, then filtering and sorting on the loaded data
/// only. Lag is not fetched here; the detail screen asks for one group's lag when it is selected.
@MainActor @Observable
public final class GroupListViewModel {
    public static let defaultStaleAfter: Duration = .seconds(30)

    public private(set) var state = LoadState<[GroupSummary]>.idle
    /// True while a reload runs over a list that is already showing.
    public private(set) var isRefreshing = false
    public var searchText = ""
    public var sortKey = GroupSortKey.id
    public var sortAscending = true
    /// Group id.
    public var selection: String?

    @ObservationIgnored private let session: ClusterSession
    @ObservationIgnored private let now: @MainActor () -> Date
    @ObservationIgnored private let staleAfter: TimeInterval
    @ObservationIgnored private var loadedAt: Date?
    @ObservationIgnored private var loadID = 0
    @ObservationIgnored private var dataRevision = 0
    @ObservationIgnored private var sortedCache: (revision: Int, key: GroupSortKey, ascending: Bool, groups: [GroupSummary])?
    @ObservationIgnored private var visibleCache: (revision: Int, key: GroupSortKey, ascending: Bool, search: String, groups: [GroupSummary])?

    public init(
        session: ClusterSession,
        staleAfter: Duration = GroupListViewModel.defaultStaleAfter,
        now: @escaping @MainActor () -> Date = { Date() }
    ) {
        self.session = session
        self.staleAfter = Double(staleAfter.components.seconds)
        self.now = now
    }

    public func belongs(to other: ClusterSession) -> Bool { session === other }

    // MARK: Loading

    /// For when the section becomes active: loads if nothing is loaded (or the last load failed), reloads quietly if
    /// the list is older than the stale limit, and does nothing for a fresh list.
    public func loadIfNeeded() async {
        switch state {
        case .loaded:
            guard let loadedAt, now().timeIntervalSince(loadedAt) > staleAfter else { return }
            await perform()
        case .idle, .failed, .loading:
            await perform()
        }
    }

    /// Reloads, keeping the current list visible while it runs.
    public func refresh() async {
        await perform()
    }

    /// A newer call supersedes an older one: only the latest result is published. If the calling task is cancelled
    /// (the section closed), the state goes back to what it was and nothing is published.
    private func perform() async {
        loadID += 1
        let id = loadID
        let previous = state
        let hadList = state.value != nil
        if hadList { isRefreshing = true } else { state = .loading }

        let result: Result<[GroupSummary], any Error>
        do {
            let client = try await session.ensureConnected()
            result = .success(try await client.listGroups())
        } catch {
            result = .failure(error)
        }
        guard id == loadID else { return }
        isRefreshing = false
        guard !Task.isCancelled else { state = previous; return }
        switch result {
        case .success(let groups):
            state = .loaded(groups)
            loadedAt = now()
            dataRevision += 1
            if let selection, !groups.contains(where: { $0.id == selection }) { self.selection = nil }
        case .failure(let error):
            state = .failed(message: Self.message(for: error))
        }
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case let error as KafkaError: error.userMessage
        case let error as ClusterSessionError: error.userMessage
        default: String(localized: "Couldn't load the consumer groups.")
        }
    }

    // MARK: Derived list

    /// Groups to show: sorted, then filtered by a case-insensitive substring of the group id.
    public var visibleGroups: [GroupSummary] {
        guard let all = state.value else { return [] }
        let search = searchText.trimmingCharacters(in: .whitespaces)
        if let cache = visibleCache, cache.revision == dataRevision, cache.key == sortKey, cache.ascending == sortAscending, cache.search == search {
            return cache.groups
        }
        let sorted: [GroupSummary]
        if let cache = sortedCache, cache.revision == dataRevision, cache.key == sortKey, cache.ascending == sortAscending {
            sorted = cache.groups
        } else {
            sorted = Self.sort(all, by: sortKey, ascending: sortAscending)
            sortedCache = (dataRevision, sortKey, sortAscending, sorted)
        }
        let groups = Self.filter(sorted, search: search)
        visibleCache = (dataRevision, sortKey, sortAscending, search, groups)
        return groups
    }

    public var totalCount: Int { state.value?.count ?? 0 }

    /// "12 groups", or "3 of 12 groups" when a filter hides some.
    public var countText: String {
        let shown = visibleGroups.count
        let total = totalCount
        return shown == total ? Self.groupCount(total) : String(localized: "\(shown) of \(Self.groupCount(total))")
    }

    public var hasNoMatches: Bool {
        state.value.map { !$0.isEmpty } == true && visibleGroups.isEmpty
    }

    private static func groupCount(_ count: Int) -> String {
        count == 1 ? String(localized: "1 group") : String(localized: "\(count) groups")
    }

    // MARK: Pure helpers

    /// Ties fall back to the group id, so the order is total and stable between reloads.
    nonisolated static func sort(_ groups: [GroupSummary], by key: GroupSortKey, ascending: Bool) -> [GroupSummary] {
        let decorated = groups.map { (lowered: $0.id.lowercased(), rank: GroupStatePresentation($0.groupState).rank, group: $0) }
        let sorted = decorated.sorted { lhs, rhs in
            let order: Int
            switch key {
            case .id: order = compare(lhs.lowered, rhs.lowered)
            case .state: order = compare(lhs.rank, rhs.rank)
            case .members: order = compare(lhs.group.memberCount, rhs.group.memberCount)
            }
            if order != 0 { return ascending ? order < 0 : order > 0 }
            return lhs.lowered != rhs.lowered ? lhs.lowered < rhs.lowered : lhs.group.id < rhs.group.id
        }
        return sorted.map(\.group)
    }

    private nonisolated static func compare<T: Comparable>(_ a: T, _ b: T) -> Int { a < b ? -1 : (a > b ? 1 : 0) }

    nonisolated static func filter(_ groups: [GroupSummary], search: String) -> [GroupSummary] {
        if search.isEmpty { return groups }
        return groups.filter { $0.id.range(of: search, options: .caseInsensitive) != nil }
    }
}
