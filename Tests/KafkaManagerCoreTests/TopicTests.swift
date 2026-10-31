import Testing
@testable import KafkaManagerCore

private func partition(replicas: [Int], isr: [Int], start: Int64 = 0, end: Int64 = 0) -> PartitionInfo {
    PartitionInfo(id: 0, leader: replicas.first, replicas: replicas, isr: isr, startOffset: start, endOffset: end)
}

@Test func partitionIsUnderReplicatedWhenIsrSmallerThanReplicas() {
    #expect(partition(replicas: [1, 2, 3], isr: [1, 2]).isUnderReplicated)
    #expect(partition(replicas: [1], isr: []).isUnderReplicated)
}

@Test func partitionIsNotUnderReplicatedWhenIsrCoversReplicas() {
    #expect(!partition(replicas: [1, 2], isr: [2, 1]).isUnderReplicated)
}

@Test func partitionMessageCountIsEndMinusStart() {
    #expect(partition(replicas: [1], isr: [1], start: 10, end: 25).messageCount == 15)
    #expect(partition(replicas: [1], isr: [1], start: 5, end: 5).messageCount == 0)
}

@Test func partitionMessageCountNeverNegative() {
    #expect(partition(replicas: [1], isr: [1], start: 30, end: 10).messageCount == 0)
}

@Test func newTopicDefaultsToEmptyConfig() {
    #expect(NewTopic(name: "t", partitions: 3, replicationFactor: 1).config.isEmpty)
}

@Suite("TopicSummary.isInternal")
struct TopicInternalTests {
    @Test(arguments: ["__consumer_offsets", "__transaction_state", "_schemas", "_confluent-metrics", "_x"])
    func underscorePrefixIsInternal(name: String) {
        #expect(TopicSummary(name: name, partitionCount: 1, replicationFactor: 1).isInternal)
    }

    @Test(arguments: ["orders", "payments.v1", "my_topic", "a__b", "", "-x", "Orders"])
    func otherNamesAreNot(name: String) {
        #expect(!TopicSummary(name: name, partitionCount: 1, replicationFactor: 1).isInternal)
    }
}

@Suite("Config and partition flags")
struct ConfigAndPartitionFlagTests {
    @Test func sensitiveEntryNeverKeepsItsValue() {
        let entry = ConfigEntry(name: "secret", value: "hunter2", isDefault: false, isSensitive: true)
        #expect(entry.value == nil)
        #expect(!"\(entry)".contains("hunter2"))
    }

    @Test func plainDictionaryIsTakenAsOverridesAndSorted() {
        let detail = TopicDetail(name: "t", partitions: [], config: ["b": "2", "a": "1"])
        #expect(detail.configEntries.map(\.name) == ["a", "b"])
        #expect(detail.configEntries.allSatisfy { !$0.isDefault && !$0.isSensitive })
        #expect(detail.config == ["a": "1", "b": "2"])
    }

    @Test func explicitEntriesDriveTheDictionaryWithoutSensitiveValues() {
        let detail = TopicDetail(name: "t", partitions: [], configEntries: [
            ConfigEntry(name: "z", value: "1", isDefault: true),
            ConfigEntry(name: "s", value: "x", isDefault: false, isSensitive: true),
        ])
        #expect(detail.configEntries.map(\.name) == ["s", "z"])
        #expect(detail.config == ["z": "1"])
    }

    @Test func offsetsAndLeaderFlags() {
        let backwards = PartitionInfo(id: 0, leader: nil, replicas: [1], isr: [1], startOffset: 10, endOffset: 4)
        #expect(backwards.hasInvalidOffsets)
        #expect(backwards.messageCount == 0)
        #expect(backwards.isLeaderless)
        #expect(PartitionInfo(id: 0, leader: -1, replicas: [1], isr: [1], startOffset: 0, endOffset: 0).isLeaderless)
        let healthy = PartitionInfo(id: 0, leader: 1, replicas: [1], isr: [1], startOffset: 2, endOffset: 7)
        #expect(!healthy.hasInvalidOffsets && !healthy.isLeaderless && healthy.messageCount == 5)
    }
}
