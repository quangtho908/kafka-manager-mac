import Testing
@testable import KafkaManagerCore

@Test func partitionLagIsEndMinusCommitted() {
    #expect(PartitionLag(topic: "t", partition: 0, committedOffset: 40, endOffset: 100).lag == 60)
    #expect(PartitionLag(topic: "t", partition: 0, committedOffset: 100, endOffset: 100).lag == 0)
}

@Test func partitionLagIsNilWhenNothingCommitted() {
    let lag = PartitionLag(topic: "t", partition: 0, committedOffset: nil, endOffset: 100)
    #expect(lag.lag == nil)
}

@Test func partitionLagNeverNegative() {
    #expect(PartitionLag(topic: "t", partition: 0, committedOffset: 120, endOffset: 100).lag == 0)
}
