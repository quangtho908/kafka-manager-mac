import Foundation
import Observation
import KafkaManagerCore

public enum TopicSortKey: Sendable, Hashable, CaseIterable {
    case name, partitions, replicationFactor
}

/// The topics of one cluster: one metadata call per load, then filtering and sorting on the loaded data only.
/// Nothing happens at init; the view asks for a load when the Topics section opens.
@MainActor @Observable
public final class TopicListViewModel {
    public static let defaultStaleAfter: Duration = .seconds(30)

    public private(set) var state = LoadState<[TopicSummary]>.idle
    /// True while a reload runs over a list that is already showing.
    public private(set) var isRefreshing = false
    public var searchText = ""
    public var showInternal = false
    public var sortKey = TopicSortKey.name
    public var sortAscending = true
    /// Topic name.
    public var selection: String?

    @ObservationIgnored private let session: ClusterSession
    @ObservationIgnored private let now: @MainActor () -> Date
    @ObservationIgnored private let staleAfter: TimeInterval
    @ObservationIgnored private var loadedAt: Date?
    @ObservationIgnored private var loadID = 0
    /// Bumped whenever `state` gets a new list, so cached results know the data changed.
    @ObservationIgnored private var dataRevision = 0
    @ObservationIgnored private var sortedCache: SortedCache?
    @ObservationIgnored private var visibleCache: VisibleCache?

    private struct SortedCache {
        let revision: Int
        let key: TopicSortKey
        let ascending: Bool
        let topics: [TopicSummary]
    }

    private struct VisibleCache {
        let revision: Int
        let key: TopicSortKey
        let ascending: Bool
        let search: String
        let showInternal: Bool
        let topics: [TopicSummary]
    }

    public init(
        session: ClusterSession,
        staleAfter: Duration = TopicListViewModel.defaultStaleAfter,
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

        let result: Result<[TopicSummary], any Error>
        do {
            let client = try await session.ensureConnected()
            result = .success(try await client.listTopics())
        } catch {
            result = .failure(error)
        }
        guard id == loadID else { return }
        isRefreshing = false
        guard !Task.isCancelled else { state = previous; return }
        switch result {
        case .success(let topics):
            state = .loaded(topics)
            loadedAt = now()
            dataRevision += 1
            if let selection, !topics.contains(where: { $0.name == selection }) { self.selection = nil }
        case .failure(let error):
            // Even a failed refresh replaces the list: stale data next to a "couldn't load" would hide the problem.
            state = .failed(message: Self.message(for: error))
        }
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case let error as KafkaError: error.userMessage
        case let error as ClusterSessionError: error.userMessage
        default: String(localized: "Couldn't load the topics.")
        }
    }

    // MARK: Derived list

    /// Topics to show: sorted, internal ones hidden unless asked for, filtered by a case-insensitive substring of the
    /// name. Sorting is redone only when the data or the sort changes; typing only runs one filter pass.
    public var visibleTopics: [TopicSummary] {
        guard let all = state.value else { return [] }
        let search = searchText.trimmingCharacters(in: .whitespaces)
        if let cache = visibleCache, cache.revision == dataRevision, cache.key == sortKey, cache.ascending == sortAscending,
           cache.search == search, cache.showInternal == showInternal {
            return cache.topics
        }
        let sorted = sortedTopics(all)
        let topics = Self.filter(sorted, search: search, showInternal: showInternal)
        visibleCache = VisibleCache(
            revision: dataRevision, key: sortKey, ascending: sortAscending, search: search, showInternal: showInternal, topics: topics)
        return topics
    }

    public var totalCount: Int { state.value?.count ?? 0 }

    /// "12 topics", or "3 of 12 topics" when a filter hides some.
    public var countText: String {
        let shown = visibleTopics.count
        let total = totalCount
        return shown == total ? Self.topicCount(total) : String(localized: "\(shown) of \(Self.topicCount(total))")
    }

    public var hasNoMatches: Bool {
        state.value.map { !$0.isEmpty } == true && visibleTopics.isEmpty
    }

    private static func topicCount(_ count: Int) -> String {
        count == 1 ? String(localized: "1 topic") : String(localized: "\(count) topics")
    }

    private func sortedTopics(_ all: [TopicSummary]) -> [TopicSummary] {
        if let cache = sortedCache, cache.revision == dataRevision, cache.key == sortKey, cache.ascending == sortAscending {
            return cache.topics
        }
        let topics = Self.sort(all, by: sortKey, ascending: sortAscending)
        sortedCache = SortedCache(revision: dataRevision, key: sortKey, ascending: sortAscending, topics: topics)
        return topics
    }

    // MARK: Pure helpers

    /// Ties fall back to the name, so the order is total and stable between reloads.
    nonisolated static func sort(_ topics: [TopicSummary], by key: TopicSortKey, ascending: Bool) -> [TopicSummary] {
        // Lowercase once per topic instead of once per comparison.
        let decorated = topics.map { (lowered: $0.name.lowercased(), topic: $0) }
        let sorted = decorated.sorted { lhs, rhs in
            let order: Int
            switch key {
            case .name: order = lhs.lowered < rhs.lowered ? -1 : (lhs.lowered > rhs.lowered ? 1 : 0)
            case .partitions: order = compare(lhs.topic.partitionCount, rhs.topic.partitionCount)
            case .replicationFactor: order = compare(lhs.topic.replicationFactor, rhs.topic.replicationFactor)
            }
            if order != 0 { return ascending ? order < 0 : order > 0 }
            return lhs.lowered != rhs.lowered ? lhs.lowered < rhs.lowered : lhs.topic.name < rhs.topic.name
        }
        return sorted.map(\.topic)
    }

    private nonisolated static func compare(_ a: Int, _ b: Int) -> Int { a < b ? -1 : (a > b ? 1 : 0) }

    nonisolated static func filter(_ topics: [TopicSummary], search: String, showInternal: Bool) -> [TopicSummary] {
        if search.isEmpty && showInternal { return topics }
        return topics.filter { topic in
            (showInternal || !topic.isInternal) && (search.isEmpty || topic.name.range(of: search, options: .caseInsensitive) != nil)
        }
    }
}
