import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private let epoch = Date(timeIntervalSince1970: 1_700_000_000)

private func message(_ partition: Int, _ offset: Int64, key: String? = nil, value: String? = "v", at seconds: Double? = nil) -> KafkaMessage {
    KafkaMessage(
        partition: partition, offset: offset, timestamp: epoch.addingTimeInterval(seconds ?? Double(offset)),
        key: key.map { Data($0.utf8) }, value: value.map { Data($0.utf8) })
}

private func seed(_ name: String, partitions: Int = 1, messages: [KafkaMessage] = []) -> FakeKafkaClient.SeedTopic {
    let infos = (0..<partitions).map { id in
        let mine = messages.filter { $0.partition == id }
        return PartitionInfo(
            id: id, leader: 1, replicas: [1], isr: [1],
            startOffset: mine.map(\.offset).min() ?? 0, endOffset: (mine.map(\.offset).max() ?? -1) + 1)
    }
    return FakeKafkaClient.SeedTopic(detail: TopicDetail(name: name, partitions: infos), messages: messages)
}

@MainActor
private struct MessagesRig {
    let fake: FakeKafkaClient
    let session: ClusterSession
    let model: MessagesViewModel
    let sleeper: SleepController?

    init(
        messages: [KafkaMessage], partitions: Int = 1, latency: Duration = .zero, byteBudget: Int = MessagesViewModel.defaultByteBudget,
        manualClock: Bool = false
    ) {
        let topic = seed("orders", partitions: partitions, messages: messages)
        let fake = FakeKafkaClient(topics: [topic], latency: latency)
        self.fake = fake
        session = ClusterSession(
            connection: ClusterConnection(name: "dev", bootstrapServers: "h:9092", environment: .local),
            secrets: InMemorySecretStore(), factory: { _ in fake })
        let infos = topic.detail.partitions
        let controller = manualClock ? SleepController() : nil
        sleeper = controller
        let realSleep: @Sendable (Duration) async throws -> Void = { try await Task.sleep(for: $0) }
        let sleep: @Sendable (Duration) async throws -> Void = controller?.sleep ?? realSleep
        model = MessagesViewModel(
            session: session, topicName: "orders", partitionInfo: { infos }, byteBudget: byteBudget, sleep: sleep)
    }

    /// Starts a fetch in a task and returns it, so a test can interact while it runs.
    func startFetch() -> Task<Void, Never> {
        let model = model
        return Task { @MainActor in await model.fetch() }
    }
}

@Suite("MessagesViewModel query") @MainActor
struct MessagesQueryTests {
    @Test func limitIsClampedAndExplained() {
        let model = MessagesRig(messages: []).model
        #expect(model.limit == 500 && model.limitNote == nil)
        model.limitInput = 0
        #expect(model.limit == 1 && model.limitNote != nil)
        model.limitInput = -5
        #expect(model.limit == 1)
        model.limitInput = 10_001
        #expect(model.limit == 10_000 && model.limitNote != nil)
        model.limitInput = 10_000
        #expect(model.limit == 10_000 && model.limitNote == nil)
    }

    @Test func invalidInputsBlockFetch() {
        let model = MessagesRig(messages: []).model
        #expect(model.canFetch)
        model.latestCount = 0
        #expect(model.queryIssues == [.latestCount] && !model.canFetch)
        model.startKind = .offset
        model.startOffset = -1
        #expect(model.queryIssues == [.offset])
        model.startOffset = 0
        model.partition = 7
        #expect(model.queryIssues == [.partition])
        model.partition = 0
        #expect(model.queryIssues.isEmpty)
    }

    @Test func offsetIsClampedToThePartitionRangeAndReported() {
        let rig = MessagesRig(messages: (5..<10).map { message(0, Int64($0)) })
        let model = rig.model
        model.startKind = .offset
        model.partition = 0
        model.startOffset = 1
        var resolved = model.resolvedQuery()
        #expect(resolved.query.start == .offset(5) && resolved.notes.count == 1)
        model.startOffset = 99
        resolved = model.resolvedQuery()
        #expect(resolved.query.start == .offset(10) && resolved.notes.count == 1)
        model.startOffset = 7
        resolved = model.resolvedQuery()
        #expect(resolved.query.start == .offset(7) && resolved.notes.isEmpty)
        model.partition = nil
        model.startOffset = 99
        #expect(model.resolvedQuery().query.start == .offset(99))
    }

    @Test func queryCarriesTopicPartitionStartAndLimit() {
        let model = MessagesRig(messages: []).model
        model.latestCount = 25
        model.limitInput = 40
        model.partition = 0
        let query = model.resolvedQuery().query
        #expect(query == MessageQuery(topic: "orders", partition: 0, start: .latest(25), limit: 40))
    }
}

@Suite("MessagesViewModel fetch") @MainActor
struct MessagesFetchTests {
    @Test func fetchesRowsAndEndsAtEndOfData() async {
        let rig = MessagesRig(messages: (0..<5).map { message(0, Int64($0)) })
        rig.model.startKind = .earliest
        await rig.model.fetch()
        #expect(rig.model.rows.count == 5)
        #expect(rig.model.status == .finished(count: 5, reason: .endOfData))
        #expect(rig.model.statusText == "Fetched 5 messages · end of data")
        #expect(rig.fake.openFetchStreams == 0)
    }

    @Test func limitEndsTheFetchAndStopsTheStream() async {
        let rig = MessagesRig(messages: (0..<50).map { message(0, Int64($0)) })
        rig.model.startKind = .earliest
        rig.model.limitInput = 7
        await rig.model.fetch()
        #expect(rig.model.rows.count == 7)
        #expect(rig.model.status == .finished(count: 7, reason: .limitReached))
        await expectSettles { rig.fake.openFetchStreams == 0 }
    }

    @Test func emptyTopicFinishesWithZero() async {
        let rig = MessagesRig(messages: [])
        await rig.model.fetch()
        #expect(rig.model.status == .finished(count: 0, reason: .endOfData))
        #expect(rig.model.rows.isEmpty)
    }

    @Test func latestReadsTheNewest() async {
        let rig = MessagesRig(messages: (0..<20).map { message(0, Int64($0)) })
        rig.model.latestCount = 3
        await rig.model.fetch()
        #expect(Set(rig.model.rows.map(\.message.offset)) == [17, 18, 19])
    }

    @Test func stopKeepsPartialRowsAndTerminatesTheStream() async {
        let rig = MessagesRig(messages: (0..<200).map { message(0, Int64($0)) }, latency: .milliseconds(2))
        rig.model.startKind = .earliest
        let task = rig.startFetch()
        await expectSettles { rig.fake.openFetchStreams == 1 }
        // Wait for at least a few rows to be published.
        for _ in 0..<200 where rig.model.rows.isEmpty { try? await Task.sleep(for: .milliseconds(10)) }
        rig.model.stop()
        await task.value
        guard case .finished(let count, .stopped) = rig.model.status else {
            Issue.record("unexpected status \(rig.model.status)")
            return
        }
        #expect(count == rig.model.rows.count && count > 0 && count < 200)
        await expectSettles { rig.fake.openFetchStreams == 0 }
        #expect(!rig.model.isFetching)
    }

    @Test func cancellingTheCallerStopsToo() async {
        let rig = MessagesRig(messages: (0..<200).map { message(0, Int64($0)) }, latency: .milliseconds(5))
        rig.model.startKind = .earliest
        let task = rig.startFetch()
        await expectSettles { rig.fake.openFetchStreams == 1 }
        task.cancel()
        await task.value
        #expect(rig.model.status == .finished(count: rig.model.rows.count, reason: .stopped))
        await expectSettles { rig.fake.openFetchStreams == 0 }
    }

    @Test func errorBeforeAnyRowFails() async {
        let rig = MessagesRig(messages: [message(0, 0)])
        await rig.fake.failing(.fetchMessages, with: .timeout)
        await rig.model.fetch()
        guard case .failed(let text, let kept) = rig.model.status else {
            Issue.record("unexpected status \(rig.model.status)")
            return
        }
        #expect(!text.isEmpty && kept == 0)
    }

    @Test func newFetchReplacesRowsAndSelection() async {
        let rig = MessagesRig(messages: (0..<10).map { message(0, Int64($0)) })
        rig.model.startKind = .earliest
        await rig.model.fetch()
        rig.model.selection = rig.model.rows[0].id
        rig.model.limitInput = 3
        await rig.model.fetch()
        #expect(rig.model.rows.count == 3 && rig.model.selection == nil)
    }

    @Test func sizeBudgetEndsTheFetch() async {
        let big = (0..<10).map { message(0, Int64($0), value: String(repeating: "x", count: 1_000)) }
        let rig = MessagesRig(messages: big, byteBudget: 3_500)
        rig.model.startKind = .earliest
        await rig.model.fetch()
        #expect(rig.model.status == .finished(count: 4, reason: .sizeLimit))
    }

    @Test func invalidQueryDoesNothing() async {
        let rig = MessagesRig(messages: [message(0, 0)])
        rig.model.latestCount = 0
        await rig.model.fetch()
        #expect(rig.model.status == .idle)
    }
}

@Suite("MessagesViewModel batching") @MainActor
struct MessagesBatchingTests {
    @Test func rowsArePublishedOnlyWhenTheClockTicksAndAtTheEnd() async {
        let rig = MessagesRig(messages: (0..<300).map { message(0, Int64($0)) }, manualClock: true)
        rig.model.startKind = .earliest
        await rig.model.fetch()
        // The clock never fired: every row arrived in the single flush at the end.
        #expect(rig.model.publishCount == 1)
        #expect(rig.model.rows.count == 300)
        let sleeper = rig.sleeper
        #expect(await sleeper?.pendingCount == 0)
        #expect(await sleeper?.requested.first == MessagesViewModel.defaultFlushInterval)
    }

    @Test func aTickPublishesWhatArrivedSoFar() async {
        let rig = MessagesRig(messages: (0..<100).map { message(0, Int64($0)) }, latency: .milliseconds(1), manualClock: true)
        rig.model.startKind = .earliest
        let task = rig.startFetch()
        await expectSettles { await rig.sleeper?.pendingCount == 1 }
        // Let some rows arrive, then tick once: they become visible while the fetch still runs.
        try? await Task.sleep(for: .milliseconds(30))
        await rig.sleeper?.fire(0)
        await expectSettles { rig.model.publishCount >= 1 }
        #expect(rig.model.rows.count > 0)
        await task.value
        #expect(rig.model.rows.count == 100)
        #expect(rig.model.publishCount <= 3)
    }

    @Test func noFlusherRunsAfterTheFetchEnds() async {
        let rig = MessagesRig(messages: (0..<3).map { message(0, Int64($0)) }, manualClock: true)
        await rig.model.fetch()
        let before = await rig.sleeper?.snapshot()
        await Task.yield()
        for _ in 0..<50 { await Task.yield() }
        #expect(await rig.sleeper?.snapshot() == before)
        #expect(await rig.sleeper?.pendingCount == 0)
    }
}

@Suite("Message rows") @MainActor
struct MessageRowsTests {
    private func row(_ partition: Int, _ offset: Int64, key: String? = nil, value: String? = "v", at seconds: Double? = nil) -> MessageRow {
        MessageRow(message(partition, offset, key: key, value: value, at: seconds))
    }

    @Test func previewsAreComputedAtIngestAndBounded() {
        let huge = String(repeating: "a", count: 300_000)
        let row = row(0, 0, key: "k", value: huge)
        #expect(row.valuePreview.text.utf8.count <= PayloadDecoder.previewLimit + 16)
        #expect(row.valuePreview.isTruncated)
        #expect(row.size == 1 + 300_000)
        #expect(!row.isTombstone)
        #expect(self.row(0, 1, value: nil).isTombstone)
    }

    @Test func sortsByEachKeyWithAStableTiebreak() {
        let rows = [row(1, 5, value: "bbbb", at: 10), row(0, 9, value: "a", at: 10), row(0, 2, value: "ccc", at: 30)]
        func ids(_ key: MessageSortKey, _ asc: Bool) -> [String] {
            MessageRows.sort(rows, by: key, ascending: asc).map { "\($0.message.partition):\($0.message.offset)" }
        }
        #expect(ids(.timestamp, false) == ["0:2", "0:9", "1:5"])
        #expect(ids(.timestamp, true) == ["0:9", "1:5", "0:2"] || ids(.timestamp, true) == ["0:9", "1:5", "0:2"])
        #expect(ids(.partition, true) == ["0:2", "0:9", "1:5"])
        #expect(ids(.offset, false) == ["0:9", "1:5", "0:2"])
        #expect(ids(.size, true) == ["0:9", "0:2", "1:5"])
    }

    @Test func filterIsCaseInsensitiveOnKeyAndValuePreview() {
        let rows = [row(0, 0, key: "User-42", value: "hello"), row(0, 1, key: "x", value: #"{"city":"Hanoi"}"#), row(0, 2, value: nil)]
        #expect(MessageRows.filter(rows, search: "user-42").count == 1)
        #expect(MessageRows.filter(rows, search: "HANOI").map(\.message.offset) == [1])
        #expect(MessageRows.filter(rows, search: "  ").count == 3)
        #expect(MessageRows.filter(rows, search: "nope").isEmpty)
    }

    @Test func tenThousandRowsSortAndFilterQuickly() {
        let rows = (0..<10_000).map { row($0 % 3, Int64($0), key: "k\($0)", value: #"{"n":\#($0)}"#, at: Double(($0 * 7919) % 10_000)) }
        let clock = ContinuousClock()
        let elapsed = clock.measure {
            let sorted = MessageRows.sort(rows, by: .timestamp, ascending: false)
            let found = MessageRows.filter(sorted, search: "k99")
            #expect(found.count == 111)
        }
        #expect(elapsed < .seconds(2))
    }
}

@Suite("MessagesViewModel derived list") @MainActor
struct MessagesDerivedListTests {
    @Test func defaultsToNewestFirstAndFiltersLive() async {
        let rig = MessagesRig(messages: (0..<5).map { message(0, Int64($0), key: "k\($0)") })
        rig.model.startKind = .earliest
        await rig.model.fetch()
        #expect(rig.model.visibleRows.map(\.message.offset) == [4, 3, 2, 1, 0])
        rig.model.searchText = "K3"
        #expect(rig.model.visibleRows.map(\.message.offset) == [3])
        rig.model.searchText = "zzz"
        #expect(rig.model.hasNoMatches)
        rig.model.searchText = ""
        rig.model.sortKey = .offset
        rig.model.sortAscending = true
        #expect(rig.model.visibleRows.map(\.message.offset) == [0, 1, 2, 3, 4])
    }
}
