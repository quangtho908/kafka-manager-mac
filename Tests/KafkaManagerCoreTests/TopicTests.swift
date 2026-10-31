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
