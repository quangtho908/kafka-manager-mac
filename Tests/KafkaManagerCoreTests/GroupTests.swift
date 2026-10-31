import Testing
@testable import KafkaManagerCore

private func lag(committed: Int64?, start: Int64 = 0, end: Int64) -> PartitionLag {
    PartitionLag(topic: "t", partition: 0, committedOffset: committed, startOffset: start, endOffset: end)
}

@Test func partitionLagIsEndMinusCommitted() {
    #expect(lag(committed: 40, end: 100).lag == 60)
    #expect(lag(committed: 100, end: 100).lag == 0)
}

@Test func partitionLagWithoutCommitIsEndMinusStart() {
    let noCommit = lag(committed: nil, start: 20, end: 100)
    #expect(!noCommit.hasCommittedOffset)
    #expect(noCommit.lag == 80)
}

@Test func partitionLagWithCommitHasCommittedOffset() {
    #expect(lag(committed: 0, end: 10).hasCommittedOffset)
}

@Test func partitionLagNeverNegative() {
    #expect(lag(committed: 120, end: 100).lag == 0)
    #expect(lag(committed: nil, start: 150, end: 100).lag == 0)
}

@Test func partitionLagFlagsCommittedAheadOfEnd() {
    #expect(lag(committed: 120, end: 100).isCommittedAheadOfEnd)
    #expect(!lag(committed: 100, end: 100).isCommittedAheadOfEnd)
    #expect(!lag(committed: nil, end: 100).isCommittedAheadOfEnd)
}
