import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private func seed(_ name: String, partitions: Int = 1, replicas: Int = 1) -> FakeKafkaClient.SeedTopic {
    let infos = (0..<partitions).map {
        PartitionInfo(id: $0, leader: 1, replicas: Array(1...max(replicas, 1)).prefix(replicas).map { $0 }, isr: [1], startOffset: 0, endOffset: 0)
    }
    return FakeKafkaClient.SeedTopic(detail: TopicDetail(name: name, partitions: infos))
}

private func summary(_ name: String, _ partitions: Int = 1, _ rf: Int = 1) -> TopicSummary {
    TopicSummary(name: name, partitionCount: partitions, replicationFactor: rf)
}

@MainActor
private final class Clock {
    var date = Date(timeIntervalSince1970: 1_000_000)
    func advance(_ seconds: TimeInterval) { date.addTimeInterval(seconds) }
}

@MainActor
private struct Rig {
    let fake: FakeKafkaClient
    let session: ClusterSession
    let clock = Clock()
    let model: TopicListViewModel
    let probe: FactoryProbe

    init(_ seeds: [FakeKafkaClient.SeedTopic]) {
        let fake = FakeKafkaClient(topics: seeds)
        self.fake = fake
        probe = FactoryProbe(make: { fake })
        session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(), factory: probe.factory)
        let clock = clock
        model = TopicListViewModel(session: session, now: { clock.date })
    }
}

@Suite("TopicListViewModel loading") @MainActor
struct TopicListViewModelLoadingTests {
    @Test func initDoesNothing() {
        let rig = Rig([seed("a")])
        #expect(rig.model.state == .idle)
        #expect(rig.probe.callCount == 0)
    }

    @Test func loadIfNeededConnectsAndLoadsFromOneMetadataCall() async {
        let rig = Rig([seed("b", partitions: 3, replicas: 2), seed("a")])
        // Offsets and details are never fetched for the list.
        await rig.fake.failing(.describeTopic, with: .timeout)
        await rig.model.loadIfNeeded()
        #expect(rig.model.state.value?.map(\.name) == ["a", "b"])
        #expect(rig.model.state.value?.last?.partitionCount == 3)
        #expect(rig.model.state.value?.last?.replicationFactor == 2)
        #expect(rig.session.status == .connected)
    }

    @Test func freshListIsNotReloaded() async throws {
        let rig = Rig([seed("a")])
        await rig.model.loadIfNeeded()
        try await rig.fake.createTopic(NewTopic(name: "b", partitions: 1, replicationFactor: 1))
        rig.clock.advance(29)
        await rig.model.loadIfNeeded()
        #expect(rig.model.state.value?.count == 1)
    }

    @Test func staleListReloadsQuietly() async throws {
        let rig = Rig([seed("a")])
        await rig.model.loadIfNeeded()
        try await rig.fake.createTopic(NewTopic(name: "b", partitions: 1, replicationFactor: 1))
        rig.clock.advance(31)
        await rig.model.loadIfNeeded()
        #expect(rig.model.state.value?.map(\.name) == ["a", "b"])
        #expect(!rig.model.isRefreshing)
    }

    @Test func refreshKeepsTheListVisibleWhileRunning() async {
        let rig = Rig([seed("a")])
        await rig.model.loadIfNeeded()
        await rig.fake.setLatency(.seconds(5))
        let task = Task { await rig.model.refresh() }
        await expectSettles { rig.model.isRefreshing }
        #expect(rig.model.state.value?.map(\.name) == ["a"])
        task.cancel()
        await task.value
        #expect(!rig.model.isRefreshing)
        #expect(rig.model.state.value?.map(\.name) == ["a"])
    }

    @Test func failureShowsTheUserMessageAndRetryLoads() async {
        let rig = Rig([seed("a")])
        await rig.fake.failing(.listTopics, with: .timeout)
        await rig.model.loadIfNeeded()
        #expect(rig.model.state.failureMessage == KafkaError.timeout.userMessage)
        await rig.fake.failing(.listTopics, with: nil)
        await rig.model.refresh()
        #expect(rig.model.state.value?.map(\.name) == ["a"])
    }

    @Test func connectFailureIsReportedToo() async {
        let fake = FakeKafkaClient()
        await fake.failing(.connect, with: .unreachable(host: "h:9092"))
        let session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(),
            factory: { _ in fake })
        let model = TopicListViewModel(session: session)
        await model.loadIfNeeded()
        #expect(model.state.failureMessage == KafkaError.unreachable(host: "h:9092").userMessage)
    }

    @Test func cancellingAFirstLoadRestoresIdleAndPublishesNothing() async {
        let rig = Rig([seed("a")])
        // Connected first: an in-flight connect is not cancelled by its waiters (documented in ClusterSession).
        _ = try? await rig.session.ensureConnected()
        await rig.fake.setLatency(.seconds(5))
        let task = Task { await rig.model.loadIfNeeded() }
        await expectSettles { rig.model.state.isLoading }
        task.cancel()
        await task.value
        #expect(rig.model.state == .idle)
    }

    @Test func aStaleResultIsIgnoredWhenANewerLoadStarted() async {
        let rig = Rig([seed("a")])
        await rig.model.loadIfNeeded()
        await rig.fake.setLatency(.seconds(5))
        let slow = Task { await rig.model.refresh() }
        await expectSettles { rig.model.isRefreshing }
        await rig.fake.setLatency(.zero)
        await rig.model.refresh()
        slow.cancel()
        await slow.value
        #expect(rig.model.state.value?.map(\.name) == ["a"])
        #expect(!rig.model.isRefreshing)
    }

    @Test func selectionOfAVanishedTopicIsCleared() async throws {
        let rig = Rig([seed("a"), seed("b")])
        await rig.model.loadIfNeeded()
        rig.model.selection = "b"
        try await rig.fake.deleteTopic("b")
        await rig.model.refresh()
        #expect(rig.model.selection == nil)
        rig.model.selection = "a"
        await rig.model.refresh()
        #expect(rig.model.selection == "a")
    }
}

@Suite("TopicListViewModel list") @MainActor
struct TopicListViewModelListTests {
    private func loaded(_ seeds: [FakeKafkaClient.SeedTopic]) async -> TopicListViewModel {
        let rig = Rig(seeds)
        await rig.model.loadIfNeeded()
        return rig.model
    }

    @Test func searchIsCaseInsensitiveSubstring() async {
        let model = await loaded([seed("Orders.v1"), seed("payments"), seed("my-orders")])
        model.searchText = "ORDERS"
        #expect(model.visibleTopics.map(\.name) == ["my-orders", "Orders.v1"])
        model.searchText = "  pay "   // surrounding spaces are not part of the search
        #expect(model.visibleTopics.map(\.name) == ["payments"])
    }

    @Test func internalTopicsAreHiddenUntilAsked() async {
        let model = await loaded([seed("__consumer_offsets"), seed("_schemas"), seed("orders")])
        #expect(model.visibleTopics.map(\.name) == ["orders"])
        model.showInternal = true
        #expect(model.visibleTopics.map(\.name) == ["__consumer_offsets", "_schemas", "orders"])
        model.searchText = "schema"
        #expect(model.visibleTopics.map(\.name) == ["_schemas"])
    }

    @Test func sortsByEachKeyInBothDirections() async {
        let model = await loaded([seed("b", partitions: 3, replicas: 1), seed("a", partitions: 1, replicas: 3), seed("c", partitions: 2, replicas: 2)])
        model.sortKey = .partitions
        #expect(model.visibleTopics.map(\.name) == ["a", "c", "b"])
        model.sortAscending = false
        #expect(model.visibleTopics.map(\.name) == ["b", "c", "a"])
        model.sortKey = .replicationFactor
        #expect(model.visibleTopics.map(\.name) == ["a", "c", "b"])
        model.sortAscending = true
        #expect(model.visibleTopics.map(\.name) == ["b", "c", "a"])
        model.sortKey = .name
        #expect(model.visibleTopics.map(\.name) == ["a", "b", "c"])
        model.sortAscending = false
        #expect(model.visibleTopics.map(\.name) == ["c", "b", "a"])
    }

    @Test func nameSortIgnoresCaseAndTiesFallBackToName() {
        let sorted = TopicListViewModel.sort([summary("b"), summary("B"), summary("a"), summary("C")], by: .name, ascending: true)
        #expect(sorted.map(\.name) == ["a", "B", "b", "C"])
        let ties = TopicListViewModel.sort([summary("z", 2), summary("y", 2), summary("x", 1)], by: .partitions, ascending: false)
        #expect(ties.map(\.name) == ["y", "z", "x"])
    }

    @Test func countTextAndNoMatches() async {
        let model = await loaded([seed("a"), seed("ab"), seed("_x")])
        #expect(model.countText == "2 of 3 topics")
        model.showInternal = true
        #expect(model.countText == "3 topics")
        model.searchText = "zzz"
        #expect(model.hasNoMatches)
        #expect(model.countText == "0 of 3 topics")
        model.searchText = "_x"
        #expect(model.countText == "1 of 3 topics")
        let single = await loaded([seed("only")])
        #expect(single.countText == "1 topic")
    }

    @Test func emptyClusterIsNotANoMatchState() async {
        let model = await loaded([])
        #expect(!model.hasNoMatches)
        #expect(model.visibleTopics.isEmpty)
        #expect(model.countText == "0 topics")
    }

    @Test func fiveThousandTopicsFilterAndSortWellUnderABudget() async {
        let topics = FakeKafkaClient.sampleData(topicCount: 5000)
        let probe = FactoryProbe(make: { topics })
        let session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(), factory: probe.factory)
        let model = TopicListViewModel(session: session)
        await model.loadIfNeeded()
        #expect(model.totalCount == 5000)

        let clock = ContinuousClock()
        let sortTime = clock.measure {
            model.sortKey = .partitions
            model.sortAscending = false
            _ = model.visibleTopics
        }
        // Typing runs only the filter pass over the already sorted list.
        let typeTime = clock.measure {
            for text in ["t", "to", "top", "topic", "topic-0", "topic-04"] {
                model.searchText = text
                _ = model.visibleTopics
            }
        }
        #expect(sortTime < .milliseconds(200), "sort took \(sortTime)")
        #expect(typeTime / 6 < .milliseconds(20), "a keystroke took \(typeTime / 6)")
        #expect(model.visibleTopics.allSatisfy { $0.name.contains("topic-04") })
    }
}
