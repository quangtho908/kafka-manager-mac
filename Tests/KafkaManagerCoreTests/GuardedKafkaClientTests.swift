import Testing
import Foundation
import os
@testable import KafkaManagerCore

/// Counts calls per method and forwards to a fake. Conforming to `KafkaClient` means adding a protocol
/// method breaks this file until the method is added to `Method` and classified below.
final class SpyKafkaClient: KafkaClient, Sendable {
    enum Method: CaseIterable, Hashable, Sendable {
        case connect, disconnect, describeCluster, listTopics, describeTopic, createTopic, deleteTopic, fetchMessages, listGroups, describeGroup, previewOffsetReset, resetOffsets, produce
    }

    /// Every write method; the guard must block these while read-only.
    static let writes: Set<Method> = [.createTopic, .deleteTopic, .resetOffsets, .produce]

    let wrapped: FakeKafkaClient
    private let counts = OSAllocatedUnfairLock(initialState: [Method: Int]())

    init(_ wrapped: FakeKafkaClient) { self.wrapped = wrapped }

    func count(_ method: Method) -> Int { counts.withLock { $0[method] ?? 0 } }
    var totalCalls: Int { counts.withLock { $0.values.reduce(0, +) } }
    private func record(_ method: Method) { counts.withLock { $0[method, default: 0] += 1 } }

    func connect(_ cluster: ClusterConnection, credentials: Credentials?) async throws {
        record(.connect)
        try await wrapped.connect(cluster, credentials: credentials)
    }
    func disconnect() async { record(.disconnect); await wrapped.disconnect() }
    func describeCluster() async throws -> ClusterInfo { record(.describeCluster); return try await wrapped.describeCluster() }
    func listTopics() async throws -> [TopicSummary] { record(.listTopics); return try await wrapped.listTopics() }
    func describeTopic(_ name: String) async throws -> TopicDetail { record(.describeTopic); return try await wrapped.describeTopic(name) }
    func createTopic(_ spec: NewTopic) async throws { record(.createTopic); try await wrapped.createTopic(spec) }
    func deleteTopic(_ name: String) async throws { record(.deleteTopic); try await wrapped.deleteTopic(name) }
    func fetchMessages(_ query: MessageQuery) -> AsyncThrowingStream<KafkaMessage, Error> {
        record(.fetchMessages)
        return wrapped.fetchMessages(query)
    }
    func listGroups() async throws -> [GroupSummary] { record(.listGroups); return try await wrapped.listGroups() }
    func describeGroup(_ id: String) async throws -> GroupDetail { record(.describeGroup); return try await wrapped.describeGroup(id) }
    func previewOffsetReset(group: String, target: OffsetResetTarget, scope: OffsetResetScope) async throws -> OffsetResetPreview {
        record(.previewOffsetReset)
        return try await wrapped.previewOffsetReset(group: group, target: target, scope: scope)
    }
    func produce(_ request: ProduceRequest) async throws -> ProduceResult {
        record(.produce)
        return try await wrapped.produce(request)
    }
    func resetOffsets(group: String, plan: OffsetResetPlan) async throws -> OffsetResetResult {
        record(.resetOffsets)
        return try await wrapped.resetOffsets(group: group, plan: plan)
    }
}

private let cluster = ClusterConnection(name: "prod-eu", bootstrapServers: "broker:9092")
private let newTopic = NewTopic(name: "fresh", partitions: 1, replicationFactor: 1)

private struct Fixture {
    let fake: FakeKafkaClient
    let spy: SpyKafkaClient
    let guarded: GuardedKafkaClient

    init(readOnly: Bool, latency: Duration = .zero) async throws {
        fake = FakeKafkaClient.sampleData(topicCount: 5)
        try await fake.connect(cluster, credentials: nil)
        await fake.setLatency(latency)
        spy = SpyKafkaClient(fake)
        guarded = GuardedKafkaClient(wrapping: spy, clusterName: "prod-eu", isReadOnly: readOnly)
    }
}

private func collect(_ stream: AsyncThrowingStream<KafkaMessage, Error>) async throws -> [KafkaMessage] {
    var result: [KafkaMessage] = []
    for try await message in stream { result.append(message) }
    return result
}

private func invoke(_ method: SpyKafkaClient.Method, on client: some KafkaClient) async throws {
    switch method {
    case .connect: try await client.connect(cluster, credentials: nil)
    case .disconnect: await client.disconnect()
    case .describeCluster: _ = try await client.describeCluster()
    case .listTopics: _ = try await client.listTopics()
    case .describeTopic: _ = try await client.describeTopic("orders")
    case .createTopic: try await client.createTopic(newTopic)
    case .deleteTopic: try await client.deleteTopic("orders")
    case .fetchMessages: _ = try await collect(client.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 5)))
    case .listGroups: _ = try await client.listGroups()
    case .describeGroup: _ = try await client.describeGroup("orders-service")
    case .previewOffsetReset: _ = try await client.previewOffsetReset(group: "orders-service", target: .latest, scope: .wholeGroup)
    case .produce: _ = try await client.produce(ProduceRequest(topic: "orders", value: Data("x".utf8)))
    case .resetOffsets: _ = try await client.resetOffsets(group: "payments-service", plan: OffsetResetPlan(assignments: []))
    }
}

// MARK: Read-only enforcement

@Test(arguments: SpyKafkaClient.Method.allCases)
func readOnlyBlocksExactlyTheWriteMethods(method: SpyKafkaClient.Method) async throws {
    let fixture = try await Fixture(readOnly: true)
    if SpyKafkaClient.writes.contains(method) {
        await #expect(throws: KafkaError.readOnly(clusterName: "prod-eu")) { try await invoke(method, on: fixture.guarded) }
        #expect(fixture.spy.totalCalls == 0)
    } else {
        try await invoke(method, on: fixture.guarded)
        #expect(fixture.spy.count(method) == 1)
    }
}

@Test func readOnlyBlocksCreateAndDeleteWithoutTouchingInner() async throws {
    let fixture = try await Fixture(readOnly: true)
    await #expect(throws: KafkaError.readOnly(clusterName: "prod-eu")) { try await fixture.guarded.createTopic(newTopic) }
    await #expect(throws: KafkaError.readOnly(clusterName: "prod-eu")) { try await fixture.guarded.deleteTopic("orders") }
    #expect(fixture.spy.count(.createTopic) == 0)
    #expect(fixture.spy.count(.deleteTopic) == 0)
    #expect(try await fixture.fake.listTopics().count == 5)
}

@Test func writableForwardsWritesOnce() async throws {
    let fixture = try await Fixture(readOnly: false)
    try await fixture.guarded.createTopic(newTopic)
    #expect(fixture.spy.count(.createTopic) == 1)
    try await fixture.guarded.deleteTopic("fresh")
    #expect(fixture.spy.count(.deleteTopic) == 1)
    #expect(try await fixture.fake.listTopics().contains { $0.name == "fresh" } == false)
}

@Test func readOnlyBlocksResetOffsetsAndLeavesTheCommitsAlone() async throws {
    let fixture = try await Fixture(readOnly: true)
    let preview = try await fixture.guarded.previewOffsetReset(group: "payments-service", target: .latest, scope: .wholeGroup)
    await #expect(throws: KafkaError.readOnly(clusterName: "prod-eu")) { try await fixture.guarded.resetOffsets(group: "payments-service", plan: preview.plan) }
    #expect(fixture.spy.count(.resetOffsets) == 0)
    #expect(try await fixture.fake.describeGroup("payments-service").lags.first?.committedOffset == 50)
}

@Test func readOnlyBlocksProduceAndNothingIsWritten() async throws {
    let fixture = try await Fixture(readOnly: true)
    let before = try await fixture.fake.describeTopic("orders").partitions.map(\.endOffset)
    await #expect(throws: KafkaError.readOnly(clusterName: "prod-eu")) { try await fixture.guarded.produce(ProduceRequest(topic: "orders", value: Data("x".utf8))) }
    #expect(fixture.spy.count(.produce) == 0)
    #expect(try await fixture.fake.describeTopic("orders").partitions.map(\.endOffset) == before)
    await fixture.guarded.setReadOnly(false)
    let result = try await fixture.guarded.produce(ProduceRequest(topic: "orders", partition: 0, value: Data("x".utf8)))
    #expect(fixture.spy.count(.produce) == 1 && result.partition == 0)
}

@Test func writeMethodClassificationCoversEveryMethod() {
    #expect(SpyKafkaClient.writes.isSubset(of: Set(SpyKafkaClient.Method.allCases)))
    #expect(SpyKafkaClient.writes == [.createTopic, .deleteTopic, .resetOffsets, .produce])
}

// MARK: Passthrough

@Test func readsReturnTheInnerClientsResultsWhileReadOnly() async throws {
    let fixture = try await Fixture(readOnly: true)
    #expect(try await fixture.guarded.describeCluster() == fixture.fake.describeCluster())
    #expect(try await fixture.guarded.listTopics() == fixture.fake.listTopics())
    #expect(try await fixture.guarded.describeTopic("orders") == fixture.fake.describeTopic("orders"))
    #expect(try await fixture.guarded.listGroups() == fixture.fake.listGroups())
    #expect(try await fixture.guarded.describeGroup("orders-service") == fixture.fake.describeGroup("orders-service"))
}

@Test func fetchMessagesPassesThroughUnchanged() async throws {
    let fixture = try await Fixture(readOnly: true)
    let query = MessageQuery(topic: "orders", start: .earliest, limit: 1000)
    let viaGuard = try await collect(fixture.guarded.fetchMessages(query))
    #expect(viaGuard.count == 90)
    #expect(viaGuard == (try await collect(fixture.fake.fetchMessages(query))))
}

@Test func cancellingFetchThroughGuardStopsStream() async throws {
    let fixture = try await Fixture(readOnly: true, latency: .milliseconds(5))
    let (signals, signal) = AsyncStream<Void>.makeStream()
    let stream = fixture.guarded.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 1000))
    let consumer = Task {
        var count = 0
        for try await _ in stream {
            count += 1
            if count == 3 { signal.yield() }
        }
        return count
    }
    for await _ in signals { break }
    consumer.cancel()
    let received = try await consumer.value
    #expect(received >= 3)
    #expect(received < 90)
}

@Test func connectAndDisconnectPassThroughWhileReadOnly() async throws {
    let fixture = try await Fixture(readOnly: true)
    await fixture.guarded.disconnect()
    await #expect(throws: KafkaError.self) { try await fixture.guarded.listTopics() }
    try await fixture.guarded.connect(cluster, credentials: nil)
    #expect(try await fixture.guarded.listTopics().count == 5)
}

// MARK: Toggling and defaults

@Test func togglingReadOnlyAppliesToTheNextCall() async throws {
    let fixture = try await Fixture(readOnly: true)
    #expect(await fixture.guarded.isReadOnly)
    await #expect(throws: KafkaError.readOnly(clusterName: "prod-eu")) { try await fixture.guarded.createTopic(newTopic) }

    await fixture.guarded.setReadOnly(false)
    #expect(await !fixture.guarded.isReadOnly)
    try await fixture.guarded.createTopic(newTopic)
    #expect(fixture.spy.count(.createTopic) == 1)

    await fixture.guarded.setReadOnly(true)
    await #expect(throws: KafkaError.readOnly(clusterName: "prod-eu")) { try await fixture.guarded.deleteTopic("fresh") }
    #expect(fixture.spy.count(.deleteTopic) == 0)
}

@Test func onlyProductionIsReadOnlyByDefault() {
    #expect(!GuardedKafkaClient.defaultReadOnly(for: .local))
    #expect(!GuardedKafkaClient.defaultReadOnly(for: .staging))
    #expect(GuardedKafkaClient.defaultReadOnly(for: .production))
}

// MARK: Error propagation

@Test func innerErrorsPropagateUnchangedWhenWritable() async throws {
    let fixture = try await Fixture(readOnly: false)
    await #expect(throws: KafkaError.topicAlreadyExists("orders")) {
        try await fixture.guarded.createTopic(NewTopic(name: "orders", partitions: 1, replicationFactor: 1))
    }
    await #expect(throws: KafkaError.unknownTopic("nope")) { try await fixture.guarded.deleteTopic("nope") }
    await fixture.fake.failing(.listTopics, with: .authorizationFailed(resource: "cluster"))
    await #expect(throws: KafkaError.authorizationFailed(resource: "cluster")) { try await fixture.guarded.listTopics() }
}

@Test func readOnlyErrorWinsOverInnerFailures() async throws {
    let fixture = try await Fixture(readOnly: true)
    await fixture.fake.failing(.createTopic, with: .timeout)
    await #expect(throws: KafkaError.readOnly(clusterName: "prod-eu")) { try await fixture.guarded.createTopic(newTopic) }
}
