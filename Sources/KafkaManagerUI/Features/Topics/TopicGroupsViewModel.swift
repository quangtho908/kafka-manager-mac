import Foundation
import Observation
import KafkaManagerCore

public struct TopicGroupRow: Identifiable, Sendable, Equatable {
    public let groupId: String
    public let state: GroupState
    /// This topic's lag for the group (sum over its partitions); 0 when the group only has it assigned.
    public let lag: Int64
    public let severity: LagSeverity
    public let hasNoOffset: Bool
    public var id: String { groupId }
}

/// Which groups consume one topic. There is no broker call for this, so it describes the groups, a few at a time, and
/// keeps the ones that have committed offsets for the topic or have it assigned. Started by the view when the tab opens
/// and cancelled when it closes.
@MainActor @Observable
public final class TopicGroupsViewModel {
    public enum Scan: Equatable, Sendable {
        case idle
        case scanning(checked: Int, total: Int)
        case finished(total: Int)
        case failed(message: String)
    }

    public let topicName: String
    public private(set) var scan = Scan.idle
    public private(set) var rows: [TopicGroupRow] = []
    /// Groups that could not be described (permissions, deleted meanwhile).
    public private(set) var unreadableCount = 0

    @ObservationIgnored private let session: ClusterSession
    @ObservationIgnored private let limit: Int
    @ObservationIgnored private var generation = 0

    public init(session: ClusterSession, topicName: String, concurrency: Int = BoundedDescriber.defaultLimit) {
        self.session = session
        self.topicName = topicName
        self.limit = concurrency
    }

    public var isScanning: Bool {
        if case .scanning = scan { true } else { false }
    }

    /// Scans every group once. Starting again replaces the previous result; cancelling the caller keeps what was found.
    public func scanGroups() async {
        generation += 1
        let id = generation
        rows = []
        unreadableCount = 0
        scan = .scanning(checked: 0, total: 0)
        let ids: [String]
        let client: any KafkaClient
        do {
            client = try await session.ensureConnected()
            ids = try await client.listGroups().map(\.id)
        } catch {
            guard id == generation, !Task.isCancelled else { return }
            scan = .failed(message: Self.message(for: error))
            return
        }
        guard id == generation, !Task.isCancelled else { return }
        scan = .scanning(checked: 0, total: ids.count)
        var checked = 0
        var found: [TopicGroupRow] = []
        var unreadable = 0
        let topic = topicName
        await BoundedDescriber.run(ids: ids, limit: limit, describe: { try await client.describeGroup($0) }) { [weak self] item in
            guard let self, id == self.generation else { return }
            checked += 1
            switch item.result {
            case .success(let detail):
                if let row = Self.row(for: detail, topic: topic) {
                    found.append(row)
                    found.sort { $0.groupId < $1.groupId }
                    self.rows = found
                }
            case .failure:
                unreadable += 1
                self.unreadableCount = unreadable
            }
            self.scan = .scanning(checked: checked, total: ids.count)
        }
        guard id == generation else { return }
        scan = Task.isCancelled ? .idle : .finished(total: ids.count)
    }

    /// nil when the group has nothing to do with the topic.
    nonisolated static func row(for detail: GroupDetail, topic: String) -> TopicGroupRow? {
        let lags = detail.lags.filter { $0.topic == topic }
        let assigned = detail.members.contains { ($0.assignments[topic] ?? []).isEmpty == false }
        guard !lags.isEmpty || assigned else { return nil }
        let total = GroupLag.total(lags)
        return TopicGroupRow(
            groupId: detail.id, state: detail.groupState, lag: total,
            severity: GroupLag.severity(of: lags), hasNoOffset: lags.contains { !$0.hasCommittedOffset })
    }

    public var progressText: String {
        switch scan {
        case .idle: ""
        case .scanning(let checked, let total): String(localized: "Checked \(checked) of \(total) groups")
        case .finished(let total): total == 1 ? String(localized: "Checked 1 group") : String(localized: "Checked \(total) groups")
        case .failed(let message): message
        }
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case let error as KafkaError: error.userMessage
        case let error as ClusterSessionError: error.userMessage
        default: String(localized: "Couldn't read the consumer groups.")
        }
    }
}
