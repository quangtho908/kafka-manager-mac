import Testing
import Foundation
@testable import KafkaManagerCore

private let cluster = ClusterConnection(name: "fake", bootstrapServers: "broker-1:9092, broker-2:9092")

private func connected(_ fake: FakeKafkaClient = FakeKafkaClient.sampleData(topicCount: 10)) async throws -> FakeKafkaClient {
    try await fake.connect(cluster, credentials: nil)
    return fake
}

private func collect(_ stream: AsyncThrowingStream<KafkaMessage, Error>) async throws -> [KafkaMessage] {
    var result: [KafkaMessage] = []
    for try await message in stream { result.append(message) }
    return result
}

// MARK: Connection

@Test func callsBeforeConnectThrowUnreachableWithConfiguredHost() async {
    let fake = FakeKafkaClient(host: "seed:9092")
    await #expect(throws: KafkaError.unreachable(host: "seed:9092")) { try await fake.listTopics() }
    await #expect(throws: KafkaError.unreachable(host: "seed:9092")) { try await fake.listGroups() }
}

@Test func callsAfterDisconnectThrowUnreachableWithFirstBootstrapServer() async throws {
    let fake = try await connected()
    await fake.disconnect()
    await #expect(throws: KafkaError.unreachable(host: "broker-1:9092")) { try await fake.listTopics() }
}

@Test func connectWithWrongCredentialsFailsAuthentication() async throws {
    let expected = Credentials(username: "alice", password: "pw")
    let fake = FakeKafkaClient(expectedCredentials: expected)
    await #expect(throws: KafkaError.authenticationFailed) { try await fake.connect(cluster, credentials: nil) }
    await #expect(throws: KafkaError.authenticationFailed) {
        try await fake.connect(cluster, credentials: Credentials(username: "alice", password: "bad"))
    }
    try await fake.connect(cluster, credentials: expected)
    _ = try await fake.listTopics()
}

// MARK: Topics

@Test func listTopicsReturnsSortedSummaries() async throws {
    let fake = try await connected()
    let topics = try await fake.listTopics()
    #expect(topics.count == 10)
    #expect(topics.map(\.name) == topics.map(\.name).sorted())
    let orders = try #require(topics.first { $0.name == "orders" })
    #expect(orders.partitionCount == 3)
    #expect(orders.replicationFactor == 3)
}

@Test func describeTopicReturnsPartitionsAndConfig() async throws {
    let fake = try await connected()
    let detail = try await fake.describeTopic("orders")
    #expect(detail.partitions.count == 3)
    #expect(detail.config["retention.ms"] == "604800000")
}

@Test func describeUnknownTopicThrowsUnknownTopic() async throws {
    let fake = try await connected()
    await #expect(throws: KafkaError.unknownTopic("nope")) { try await fake.describeTopic("nope") }
}

@Test func createTopicIsVisibleInListAndDescribe() async throws {
    let fake = try await connected()
    try await fake.createTopic(NewTopic(name: "fresh", partitions: 4, replicationFactor: 2, config: ["cleanup.policy": "compact"]))
    let detail = try await fake.describeTopic("fresh")
    #expect(detail.partitions.count == 4)
    #expect(detail.partitions.allSatisfy { $0.replicas.count == 2 && !$0.isUnderReplicated && $0.messageCount == 0 })
    #expect(detail.config == ["cleanup.policy": "compact"])
    #expect(try await fake.listTopics().contains { $0.name == "fresh" })
}

@Test func createExistingTopicThrowsAlreadyExists() async throws {
    let fake = try await connected()
    await #expect(throws: KafkaError.topicAlreadyExists("orders")) {
        try await fake.createTopic(NewTopic(name: "orders", partitions: 1, replicationFactor: 1))
    }
}

@Test func deleteTopicRemovesItAndItsMessages() async throws {
    let fake = try await connected()
    try await fake.deleteTopic("orders")
    #expect(try await fake.listTopics().contains { $0.name == "orders" } == false)
    await #expect(throws: KafkaError.unknownTopic("orders")) { try await fake.describeTopic("orders") }
    await #expect(throws: KafkaError.unknownTopic("orders")) {
        try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 5)))
    }
}

@Test func deleteUnknownTopicThrowsUnknownTopic() async throws {
    let fake = try await connected()
    await #expect(throws: KafkaError.unknownTopic("nope")) { try await fake.deleteTopic("nope") }
}

// MARK: Failure injection

@Test func failNextFailsExactlyOneCall() async throws {
    let fake = try await connected()
    await fake.failNext(.timeout)
    await #expect(throws: KafkaError.timeout) { try await fake.listTopics() }
    #expect(try await fake.listTopics().count == 10)
}

@Test func failNextAppliesToConnect() async {
    let fake = FakeKafkaClient()
    await fake.failNext(.unreachable(host: "x:1"))
    await #expect(throws: KafkaError.unreachable(host: "x:1")) { try await fake.connect(cluster, credentials: nil) }
}

@Test func failingMethodPersistsUntilCleared() async throws {
    let fake = try await connected()
    await fake.failing(.listTopics, with: .authorizationFailed(resource: "cluster"))
    for _ in 0..<3 {
        await #expect(throws: KafkaError.authorizationFailed(resource: "cluster")) { try await fake.listTopics() }
    }
    _ = try await fake.listGroups()
    await fake.failing(.listTopics, with: nil)
    #expect(try await fake.listTopics().count == 10)
}

@Test func failureAppliesToFetchStream() async throws {
    let fake = try await connected()
    await fake.failing(.fetchMessages, with: .timeout)
    await #expect(throws: KafkaError.timeout) {
        try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 5)))
    }
}

// MARK: Messages

@Test func fetchEarliestReturnsPartitionThenOffsetOrderUpToLimit() async throws {
    let fake = try await connected()
    let all = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 1000)))
    #expect(all.count == 90)
    #expect(all.map { [$0.partition, Int($0.offset)] } == all.map { [$0.partition, Int($0.offset)] }.sorted { ($0[0], $0[1]) < ($1[0], $1[1]) })

    let limited = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 7)))
    #expect(limited == Array(all.prefix(7)))
}

@Test func fetchPartitionFilterOnlyReturnsThatPartition() async throws {
    let fake = try await connected()
    let result = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", partition: 1, start: .earliest, limit: 1000)))
    #expect(result.count == 30)
    #expect(result.allSatisfy { $0.partition == 1 })
    #expect(result.map(\.offset) == Array(0..<30))
}

@Test func fetchFromOffsetSkipsEarlierMessages() async throws {
    let fake = try await connected()
    let result = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", partition: 0, start: .offset(25), limit: 1000)))
    #expect(result.map(\.offset) == [25, 26, 27, 28, 29])
}

@Test func fetchFromTimestampSkipsOlderMessages() async throws {
    let fake = try await connected()
    let all = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", partition: 0, start: .earliest, limit: 1000)))
    let cutoff = all[27].timestamp
    let result = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", partition: 0, start: .timestamp(cutoff), limit: 1000)))
    #expect(result.map(\.offset) == [27, 28, 29])
}

@Test func fetchLatestReturnsNewestMessagesOrderedByOffset() async throws {
    let fake = try await connected()
    let single = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", partition: 0, start: .latest(3), limit: 1000)))
    #expect(single.map(\.offset) == [27, 28, 29])

    // Across partitions the sample timestamps interleave (offset * 3 + partition), so the newest 3 are offset 29 of each.
    let multi = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .latest(3), limit: 1000)))
    #expect(multi.map { [$0.partition, Int($0.offset)] } == [[0, 29], [1, 29], [2, 29]])
}

@Test func fetchLatestIsCappedByLimit() async throws {
    let fake = try await connected()
    let result = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", partition: 0, start: .latest(10), limit: 2)))
    #expect(result.map(\.offset) == [28, 29])
}

@Test func fetchWithZeroLimitOrNoMessagesFinishesEmpty() async throws {
    let fake = try await connected()
    #expect(try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 0))).isEmpty)
    #expect(try await collect(fake.fetchMessages(MessageQuery(topic: "topic-00005", start: .earliest, limit: 10))).isEmpty)
    #expect(try await collect(fake.fetchMessages(MessageQuery(topic: "orders", partition: 9, start: .earliest, limit: 10))).isEmpty)
}

@Test func fetchBeforeConnectAndUnknownTopicFail() async throws {
    let fake = FakeKafkaClient.sampleData(topicCount: 10)
    await #expect(throws: KafkaError.unreachable(host: "localhost:9092")) {
        try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 1)))
    }
    try await fake.connect(cluster, credentials: nil)
    await #expect(throws: KafkaError.unknownTopic("nope")) {
        try await collect(fake.fetchMessages(MessageQuery(topic: "nope", start: .earliest, limit: 1)))
    }
}

@Test func fetchDoesNotChangeGroups() async throws {
    let fake = try await connected()
    let summariesBefore = try await fake.listGroups()
    let detailsBefore = try await [fake.describeGroup("orders-service"), fake.describeGroup("payments-service")]
    _ = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 1000)))
    _ = try await collect(fake.fetchMessages(MessageQuery(topic: "payments", start: .latest(5), limit: 5)))
    #expect(try await fake.listGroups() == summariesBefore)
    #expect(try await [fake.describeGroup("orders-service"), fake.describeGroup("payments-service")] == detailsBefore)
}

@Test func cancellingConsumerStopsStreamEarly() async throws {
    let fake = try await connected(FakeKafkaClient.sampleData(topicCount: 2))
    await fake.setLatency(.milliseconds(5))
    let (signals, signal) = AsyncStream<Void>.makeStream()
    let stream = fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 1000))
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

// MARK: Groups

@Test func listGroupsReturnsSortedSummaries() async throws {
    let fake = try await connected()
    let groups = try await fake.listGroups()
    #expect(groups.map(\.id) == groups.map(\.id).sorted())
    #expect(groups.count == FakeKafkaClient.sampleGroupCount)
    #expect(groups.first { $0.id == "orders-service" }?.memberCount == 2)
    #expect(groups.contains { $0.id == "payments-service" })
}

@Test func describeGroupExposesNoCommitAndCommittedAheadCases() async throws {
    let fake = try await connected()
    let orders = try await fake.describeGroup("orders-service")
    let noCommit = try #require(orders.lags.first { $0.partition == 2 })
    #expect(!noCommit.hasCommittedOffset)
    #expect(noCommit.lag == 30)
    let payments = try await fake.describeGroup("payments-service")
    #expect(payments.lags.first?.isCommittedAheadOfEnd == true)
}

@Test func describeUnknownGroupThrowsClusterError() async throws {
    let fake = try await connected()
    await #expect(throws: KafkaError.clusterError(code: 69, message: "Group nope not found")) {
        try await fake.describeGroup("nope")
    }
}

// MARK: Scale and concurrency

@Test func sampleData5000TopicsListsQuickly() async throws {
    let fake = FakeKafkaClient.sampleData()
    try await fake.connect(cluster, credentials: nil)
    var topics: [TopicSummary] = []
    let elapsed = try await ContinuousClock().measure { topics = try await fake.listTopics() }
    #expect(topics.count == 5000)
    #expect(Set(topics.map(\.name)).count == 5000)
    #expect(elapsed < .seconds(1))
}

@Test func sampleDataIsDeterministic() async throws {
    let first = try await connected(FakeKafkaClient.sampleData(topicCount: 50))
    let second = try await connected(FakeKafkaClient.sampleData(topicCount: 50))
    #expect(try await first.listTopics() == second.listTopics())
    #expect(try await first.describeGroup("orders-service") == second.describeGroup("orders-service"))
}

@Test func parallelListTopicsAreSafe() async throws {
    let fake = try await connected(FakeKafkaClient.sampleData(topicCount: 200))
    let counts = try await withThrowingTaskGroup(of: Int.self) { group in
        for _ in 0..<50 { group.addTask { try await fake.listTopics().count } }
        return try await group.reduce(into: []) { $0.append($1) }
    }
    #expect(counts.count == 50)
    #expect(counts.allSatisfy { $0 == 200 })
}

@Test func parallelCreatesOfSameTopicYieldExactlyOneSuccess() async throws {
    let fake = try await connected()
    let successes = await withTaskGroup(of: Bool.self) { group in
        for _ in 0..<20 {
            group.addTask { (try? await fake.createTopic(NewTopic(name: "race", partitions: 1, replicationFactor: 1))) != nil }
        }
        return await group.reduce(0) { $0 + ($1 ? 1 : 0) }
    }
    #expect(successes == 1)
}

@Test func describeClusterReportsBrokerCountAndNeedsAConnection() async throws {
    let fake = FakeKafkaClient(brokerCount: 5)
    await #expect(throws: KafkaError.self) { try await fake.describeCluster() }
    try await fake.connect(ClusterConnection(name: "c", bootstrapServers: "h:9092"), credentials: nil)
    #expect(try await fake.describeCluster() == ClusterInfo(brokerCount: 5))
}

@Test func sampleDataHasUnhealthyAndCustomisedTopicsDeterministically() async throws {
    let client = FakeKafkaClient.sampleData(topicCount: 60)
    try await client.connect(ClusterConnection(name: "c", bootstrapServers: "h:9092"), credentials: nil)
    var underReplicated = 0, leaderless = 0, nonDefault = 0
    for topic in try await client.listTopics() {
        let detail = try await client.describeTopic(topic.name)
        underReplicated += detail.partitions.filter(\.isUnderReplicated).count
        leaderless += detail.partitions.filter(\.isLeaderless).count
        nonDefault += detail.configEntries.filter { !$0.isDefault }.count
    }
    #expect(underReplicated > 0 && leaderless > 0 && nonDefault > 0)
    let payments = try await client.describeTopic("payments")
    #expect(payments.configEntries.contains { $0.isSensitive && $0.value == nil })
    #expect(!payments.config.values.contains("never-shown"))
}

private func connectedFake(topics: [FakeKafkaClient.SeedTopic] = []) async throws -> FakeKafkaClient {
    let fake = FakeKafkaClient(topics: topics)
    try await fake.connect(cluster, credentials: nil)
    return fake
}

@Test func createTopicRejectsNonPositivePartitionsAndReplicationFactor() async throws {
    let fake = try await connectedFake()
    for partitions in [0, -1] {
        await #expect(throws: KafkaError.clusterError(code: KafkaError.invalidPartitionsCode, message: "Invalid number of partitions")) {
            try await fake.createTopic(NewTopic(name: "t", partitions: partitions, replicationFactor: 1))
        }
    }
    for factor in [0, -1, 4] {
        await #expect(throws: KafkaError.clusterError(code: KafkaError.invalidReplicationFactorCode, message: "Invalid replication factor")) {
            try await fake.createTopic(NewTopic(name: "t", partitions: 1, replicationFactor: factor))
        }
    }
    #expect(try await fake.listTopics().isEmpty)
}

@Test func createTopicRejectsInvalidNames() async throws {
    let fake = try await connectedFake()
    for name in ["", "..", "a b", String(repeating: "x", count: 250)] {
        await #expect(throws: KafkaError.clusterError(code: KafkaError.invalidTopicCode, message: "Invalid topic name")) {
            try await fake.createTopic(NewTopic(name: name, partitions: 1, replicationFactor: 1))
        }
    }
}

@Test func validationErrorsHaveTheirOwnUserMessages() {
    let codes = [KafkaError.invalidTopicCode, KafkaError.invalidPartitionsCode, KafkaError.invalidReplicationFactorCode]
    let messages = codes.map { KafkaError.clusterError(code: $0, message: "raw").userMessage }
    #expect(Set(messages).count == 3)
    #expect(messages.allSatisfy { !$0.contains("code") && !$0.contains("raw") })
}

@Test func unknownGroupHasAFriendlyUserMessage() {
    let message = KafkaError.clusterError(code: KafkaError.groupIdNotFoundCode, message: "raw").userMessage
    #expect(message.contains("group") && !message.contains("code") && !message.contains("raw"))
}

@Test func failedConnectLeavesTheClientDisconnected() async throws {
    let fake = try await connectedFake(topics: [])
    await fake.failing(.connect, with: .timeout)
    await #expect(throws: KafkaError.timeout) { try await fake.connect(cluster, credentials: nil) }
    await fake.failing(.connect, with: nil)
    await #expect(throws: KafkaError.self) { try await fake.listTopics() }
}

@Test func createAndDeleteShowUpInTheNextListTopics() async throws {
    let fake = try await connectedFake()
    try await fake.createTopic(NewTopic(name: "fresh", partitions: 2, replicationFactor: 1))
    #expect(try await fake.listTopics().map(\.name) == ["fresh"])
    try await fake.deleteTopic("fresh")
    #expect(try await fake.listTopics().isEmpty)
}

@Test func failNextFetchDeliversNMessagesThenThrowsOnceAndClosesTheStream() async throws {
    let fake = try await connected()
    await fake.failNextFetch(after: 3, with: .timeout)
    var received: [KafkaMessage] = []
    var thrown: KafkaError?
    do {
        for try await message in fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 10)) { received.append(message) }
    } catch {
        thrown = error as? KafkaError
    }
    #expect(received.count == 3)
    #expect(thrown == .timeout)
    #expect(fake.openFetchStreams == 0)
    // One-shot: the next fetch completes.
    let again = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 10)))
    #expect(again.count == 10)
}

@Test func failNextFetchStaysArmedWhenTheStreamIsShorter() async throws {
    let fake = try await connected()
    await fake.failNextFetch(after: 50, with: .timeout)
    let short = try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 5)))
    #expect(short.count == 5)
    await #expect(throws: KafkaError.timeout) {
        try await collect(fake.fetchMessages(MessageQuery(topic: "orders", start: .earliest, limit: 100)))
    }
}
