import Foundation
import Observation
import KafkaManagerCore

/// One topic's partitions and config. Loaded when the topic is selected, never for the whole list.
@MainActor @Observable
public final class TopicDetailViewModel {
    public let topicName: String
    public private(set) var state = LoadState<TopicDetail>.idle
    /// True while a reload runs over data that is already showing.
    public private(set) var isRefreshing = false

    @ObservationIgnored private let session: ClusterSession
    /// Called once when the broker says the topic no longer exists, so the list can refresh and drop it.
    @ObservationIgnored private let onTopicGone: @MainActor (String) async -> Void
    @ObservationIgnored private var loadID = 0

    public init(session: ClusterSession, topicName: String, onTopicGone: @escaping @MainActor (String) async -> Void = { _ in }) {
        self.session = session
        self.topicName = topicName
        self.onTopicGone = onTopicGone
    }

    public func loadIfNeeded() async {
        if state.value == nil { await perform() }
    }

    /// Reloads, keeping the current data visible while it runs.
    public func refresh() async {
        await perform()
    }

    /// The latest call wins. A cancelled caller (another topic was selected) restores the previous state and
    /// publishes nothing.
    private func perform() async {
        loadID += 1
        let id = loadID
        let previous = state
        if state.value != nil { isRefreshing = true } else { state = .loading }

        let result: Result<TopicDetail, any Error>
        do {
            let client = try await session.ensureConnected()
            result = .success(try await client.describeTopic(topicName))
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
            state = .failed(message: Self.message(for: error))
            if case KafkaError.unknownTopic = error { await onTopicGone(topicName) }
        }
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case KafkaError.unknownTopic(let name): String(localized: "The topic \(name) no longer exists. It may have been deleted.")
        case let error as KafkaError: error.userMessage
        case let error as ClusterSessionError: error.userMessage
        default: String(localized: "Couldn't load the topic.")
        }
    }
}
