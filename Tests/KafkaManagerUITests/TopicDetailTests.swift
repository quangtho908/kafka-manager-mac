import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private func partition(_ id: Int, leader: Int? = 1, replicas: [Int] = [1, 2, 3], isr: [Int] = [1, 2, 3], start: Int64 = 0, end: Int64 = 0) -> PartitionInfo {
    PartitionInfo(id: id, leader: leader, replicas: replicas, isr: isr, startOffset: start, endOffset: end)
}

@Suite("Partition and config presentation")
struct TopicDetailPresentationTests {
    @Test func healthyPartitionHasNoFlags() {
        let row = PartitionPresentation.row(partition(0, start: 5, end: 12))
        #expect(row.isHealthy)
        #expect(row.messageCount == 7)
        #expect(row.leaderText == "1")
        #expect(row.replicasText == "1, 2, 3")
    }

    @Test func underReplicatedWhenIsrIsSmallerThanReplicas() {
        #expect(PartitionPresentation.row(partition(0, isr: [1, 2])).flags == [.underReplicated])
        #expect(PartitionPresentation.row(partition(0, replicas: [1], isr: [1])).isHealthy)
    }

    @Test func brokersMissingFromTheIsrAreNamedInOrder() {
        let row = PartitionPresentation.row(partition(0, replicas: [3, 1, 2], isr: [1]))
        #expect(row.outOfSync == [3, 2])
        #expect(row.outOfSyncText == "out of sync: 3, 2")
        #expect(PartitionPresentation.row(partition(0)).outOfSyncText == nil)
        // A broker in the ISR that is not a replica any more is not "missing".
        #expect(PartitionPresentation.row(partition(0, replicas: [1], isr: [1, 9])).outOfSync.isEmpty)
    }

    @Test func leaderlessWhenNilOrMinusOne() {
        for leader in [nil, -1] as [Int?] {
            let row = PartitionPresentation.row(partition(0, leader: leader))
            #expect(row.flags == [.noLeader])
            #expect(row.leaderText == "—")
        }
    }

    @Test func backwardsOffsetsAreClampedAndFlagged() {
        let row = PartitionPresentation.row(partition(0, start: 10, end: 4))
        #expect(row.messageCount == 0)
        #expect(row.flags == [.invalidOffsets])
    }

    @Test func flagsCombineAndEachHasTextAndSymbol() {
        let row = PartitionPresentation.row(partition(0, leader: nil, isr: []))
        #expect(row.flags == [.underReplicated, .noLeader])
        #expect(row.flagsText.contains(PartitionFlag.underReplicated.label))
        #expect(row.flagsText.contains(PartitionFlag.noLeader.label))
        #expect(Set(PartitionFlag.allCases.map(\.label)).count == 3)
        #expect(Set(PartitionFlag.allCases.map(\.symbolName)).count == 3)
    }

    @Test func rowsAreOrderedByIdAndSummarised() {
        let rows = PartitionPresentation.rows([partition(2, leader: nil), partition(0), partition(1, isr: [1])])
        #expect(rows.map(\.id) == [0, 1, 2])
        #expect(PartitionPresentation.summary(rows) == "1 under-replicated, 1 without a leader")
        #expect(PartitionPresentation.summary(PartitionPresentation.rows([partition(0)])) == nil)
    }

    @Test func configPutsNonDefaultFirstThenByName() {
        let rows = ConfigPresentation.rows([
            ConfigEntry(name: "b", value: "1", isDefault: true),
            ConfigEntry(name: "z", value: "2", isDefault: false),
            ConfigEntry(name: "a", value: "3", isDefault: true),
            ConfigEntry(name: "c", value: "4", isDefault: false),
        ])
        #expect(rows.map(\.name) == ["c", "z", "a", "b"])
        #expect(rows.map(\.isNonDefault) == [true, true, false, false])
        #expect(ConfigPresentation.rows([ConfigEntry(name: "b", value: "1", isDefault: true), ConfigEntry(name: "c", value: "4", isDefault: false)], onlyNonDefault: true).map(\.name) == ["c"])
    }

    @Test func sensitiveValueIsHiddenAndMissingValueIsADash() {
        let rows = ConfigPresentation.rows([
            ConfigEntry(name: "s", value: "hunter2", isDefault: false, isSensitive: true),
            ConfigEntry(name: "n", value: nil, isDefault: true),
        ])
        #expect(rows.first { $0.name == "s" }?.valueText == ConfigPresentation.hiddenValue)
        #expect(!"\(rows)".contains("hunter2"))
        #expect(rows.first { $0.name == "n" }?.valueText == "—")
    }
}

@MainActor
private struct DetailRig {
    let fake: FakeKafkaClient
    let session: ClusterSession
    let gone = GoneRecorder()

    init(_ names: [String] = ["orders"]) {
        let fake = FakeKafkaClient(topics: names.map {
            FakeKafkaClient.SeedTopic(detail: TopicDetail(name: $0, partitions: [partition(0), partition(1, isr: [1])], config: ["retention.ms": "1"]))
        })
        self.fake = fake
        session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(),
            factory: { _ in fake })
    }

    func model(_ topic: String = "orders") -> TopicDetailViewModel {
        let gone = gone
        return TopicDetailViewModel(session: session, topicName: topic) { name in gone.names.append(name) }
    }
}

@MainActor
private final class GoneRecorder { var names: [String] = [] }

@Suite("TopicDetailViewModel") @MainActor
struct TopicDetailViewModelTests {
    @Test func initDoesNothing() {
        let rig = DetailRig()
        #expect(rig.model().state == .idle)
        #expect(rig.session.status == .idle)
    }

    @Test func loadsOnlyTheSelectedTopicLazily() async {
        let rig = DetailRig(["orders", "payments"])
        let model = rig.model()
        await model.loadIfNeeded()
        #expect(model.state.value?.name == "orders")
        #expect(model.state.value?.partitions.count == 2)
        // A second call keeps the data and does not reload.
        try? await rig.fake.deleteTopic("orders")
        await model.loadIfNeeded()
        #expect(model.state.value?.name == "orders")
    }

    @Test func refreshKeepsDataWhileRunning() async {
        let rig = DetailRig()
        let model = rig.model()
        await model.loadIfNeeded()
        await rig.fake.setLatency(.seconds(5))
        let task = Task { await model.refresh() }
        await expectSettles { model.isRefreshing }
        #expect(model.state.value?.name == "orders")
        task.cancel()
        await task.value
        #expect(!model.isRefreshing)
        #expect(model.state.value?.name == "orders")
    }

    @Test func failureShowsTheMessageAndRetryWorks() async {
        let rig = DetailRig()
        await rig.fake.failing(.describeTopic, with: .authorizationFailed(resource: "topic orders"))
        let model = rig.model()
        await model.loadIfNeeded()
        #expect(model.state.failureMessage == KafkaError.authorizationFailed(resource: "topic orders").userMessage)
        await rig.fake.failing(.describeTopic, with: nil)
        await model.refresh()
        #expect(model.state.value?.name == "orders")
        #expect(rig.gone.names.isEmpty)
    }

    @Test func cancelledFirstLoadPublishesNothing() async {
        let rig = DetailRig()
        _ = try? await rig.session.ensureConnected()
        await rig.fake.setLatency(.seconds(5))
        let model = rig.model()
        let task = Task { await model.loadIfNeeded() }
        await expectSettles { model.state.isLoading }
        task.cancel()
        await task.value
        #expect(model.state == .idle)
    }

    @Test func unknownTopicSaysSoAndTellsTheListOnce() async {
        let rig = DetailRig(["orders"])
        let model = rig.model("gone")
        await model.loadIfNeeded()
        #expect(model.state.failureMessage?.contains("gone") == true)
        #expect(rig.gone.names == ["gone"])
    }
}

@Suite("Deleted topic and the list") @MainActor
struct DeletedTopicListTests {
    @Test func listRefreshesOnceClearsSelectionAndShowsANotice() async throws {
        let rig = DetailRig(["orders", "payments"])
        let list = TopicListViewModel(session: rig.session)
        await list.loadIfNeeded()
        list.selection = "payments"
        let detail = list.detailModel(for: "payments")
        #expect(list.detailModel(for: "payments") === detail)
        await detail.loadIfNeeded()

        try await rig.fake.deleteTopic("payments")
        await detail.refresh()
        #expect(list.selection == nil)
        #expect(list.state.value?.map(\.name) == ["orders"])
        #expect(list.removedTopicNotice?.contains("payments") == true)
        list.dismissRemovedTopicNotice()
        #expect(list.removedTopicNotice == nil)
    }

    @Test func onlyTheCurrentDetailModelIsKept() async {
        let rig = DetailRig(["a", "b"])
        let list = TopicListViewModel(session: rig.session)
        let a = list.detailModel(for: "a")
        let b = list.detailModel(for: "b")
        #expect(a !== b)
        #expect(list.detailModel(for: "a") !== a)
    }
}
