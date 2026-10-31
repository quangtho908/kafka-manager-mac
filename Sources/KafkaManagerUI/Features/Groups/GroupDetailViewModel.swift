import Foundation
import Observation
import KafkaManagerCore

public enum AutoRefreshInterval: Int, CaseIterable, Identifiable, Sendable {
    case five = 5, ten = 10, thirty = 30

    public var id: Int { rawValue }
    public var duration: Duration { .seconds(rawValue) }
    public var title: String { String(localized: "\(rawValue) s") }
}

/// What the auto-refresh task is keyed on: it restarts when either changes and does not exist while off.
public struct AutoRefreshKey: Hashable, Sendable {
    public let isOn: Bool
    public let interval: AutoRefreshInterval
}

/// One group's detail. Loaded when the group is selected (one `describeGroup` call), never for the whole list.
@MainActor @Observable
public final class GroupDetailViewModel {
    public enum Tab: Hashable, Sendable { case lag, members }

    public let groupId: String
    public private(set) var state = LoadState<GroupDetail>.idle
    /// True while a reload runs over data that is already showing.
    public private(set) var isRefreshing = false
    public var tab = Tab.lag
    /// Off by default. The view runs `autoRefresh()` in a task keyed on `autoRefreshKey` only while it is on screen.
    public var autoRefreshOn = false
    public var autoRefreshInterval = AutoRefreshInterval.ten
    public var thresholds = LagThresholds.standard

    @ObservationIgnored private let session: ClusterSession
    @ObservationIgnored private let onGroupGone: @MainActor (String) async -> Void
    @ObservationIgnored private let sleep: @Sendable (Duration) async throws -> Void
    @ObservationIgnored private var loadID = 0

    public init(
        session: ClusterSession, groupId: String,
        sleep: @escaping @Sendable (Duration) async throws -> Void = { try await Task.sleep(for: $0) },
        onGroupGone: @escaping @MainActor (String) async -> Void = { _ in }
    ) {
        self.session = session
        self.groupId = groupId
        self.sleep = sleep
        self.onGroupGone = onGroupGone
    }

    // MARK: Loading

    public func loadIfNeeded() async {
        if state.value == nil { await perform() }
    }

    /// Reloads, keeping the current data visible while it runs.
    public func refresh() async {
        await perform()
    }

    /// The latest call wins. A cancelled caller (another group was selected) restores the previous state and
    /// publishes nothing. A group that no longer exists reports itself once, so the list can drop it.
    private func perform() async {
        loadID += 1
        let id = loadID
        let previous = state
        if state.value != nil { isRefreshing = true } else { state = .loading }

        let result: Result<GroupDetail, any Error>
        do {
            let client = try await session.ensureConnected()
            result = .success(try await client.describeGroup(groupId))
        } catch {
            result = .failure(error)
        }
        guard id == loadID else { return }
        isRefreshing = false
        guard !Task.isCancelled else { state = previous; return }
        switch result {
        case .success(let detail):
            state = .loaded(detail)
        case .failure(let error):
            state = .failed(message: message(for: error))
            if let error = error as? KafkaError, error.isUnknownGroup { await onGroupGone(groupId) }
        }
    }

    private func message(for error: any Error) -> String {
        switch error {
        case let error as KafkaError where error.isUnknownGroup:
            String(localized: "The group \(groupId) no longer exists. It may have been deleted.")
        case let error as KafkaError: error.userMessage
        case let error as ClusterSessionError: error.userMessage
        default: String(localized: "Couldn't load the group.")
        }
    }

    // MARK: Auto refresh

    public var autoRefreshKey: AutoRefreshKey { AutoRefreshKey(isOn: autoRefreshOn, interval: autoRefreshInterval) }

    /// Sleeps, refreshes, repeats, until cancelled (the view left, the selection changed, the toggle or interval
    /// changed), the toggle is off, the connection is gone, or the group no longer exists. A tick that finds the
    /// previous refresh still running is skipped.
    public func autoRefresh() async {
        while autoRefreshOn, !Task.isCancelled {
            do { try await sleep(autoRefreshInterval.duration) } catch { return }
            guard autoRefreshOn, !Task.isCancelled else { return }
            guard session.status == .connected else {
                autoRefreshOn = false
                return
            }
            if !isRefreshing { await perform() }
            if case .failed = state { autoRefreshOn = false }
        }
    }

    // MARK: Derived

    public var header: GroupHeaderInfo? {
        state.value.map { GroupDetailPresentation.header($0, thresholds: thresholds) }
    }

    public var lagSections: [LagSection] {
        state.value.map { GroupDetailPresentation.lagSections($0, thresholds: thresholds) } ?? []
    }

    public var memberRows: [MemberRow] {
        state.value.map(GroupDetailPresentation.memberRows) ?? []
    }
}
