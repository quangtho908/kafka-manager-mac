import Testing
import Foundation
@testable import KafkaManagerCore

@Test func messageQueryDefaultsToAllPartitions() {
    #expect(MessageQuery(topic: "t", start: .earliest, limit: 10).partition == nil)
}

@Test func startPositionsAreDistinguishedByCaseAndPayload() {
    #expect(StartPosition.latest(50) == .latest(50))
    #expect(StartPosition.latest(50) != .latest(51))
    #expect(StartPosition.offset(5) != .earliest)
    #expect(StartPosition.timestamp(Date(timeIntervalSince1970: 1)) != .timestamp(Date(timeIntervalSince1970: 2)))
}

@Test func kafkaMessageKeepsNilKeyAndValueDistinctFromEmpty() {
    let date = Date(timeIntervalSince1970: 0)
    let tombstone = KafkaMessage(partition: 0, offset: 1, timestamp: date, key: Data("k".utf8), value: nil)
    let empty = KafkaMessage(partition: 0, offset: 1, timestamp: date, key: Data("k".utf8), value: Data())
    #expect(tombstone != empty)
    #expect(tombstone.headers.isEmpty)
}
