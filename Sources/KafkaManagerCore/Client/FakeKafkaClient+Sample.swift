import Foundation

extension FakeKafkaClient {
    private static let sampleEpoch = Date(timeIntervalSince1970: 1_700_000_000)
    private static let samplePartitions = 3
    public static let sampleMessagesPerPartition = 30

    /// Deterministic dataset for previews and perf checks. The first two topics (`orders`, `payments`)
    /// carry messages and consumer groups: one partition with no commit, one committed past the log end.
    public static func sampleData(topicCount: Int = 5000, messagesPerPartition: Int = sampleMessagesPerPartition) -> FakeKafkaClient {
        var seeds: [SeedTopic] = (0..<topicCount).map { index in
            switch index {
            case 0: sampleTopic(name: "orders", messagesPerPartition: messagesPerPartition)
            case 1: sampleTopic(name: "payments", messagesPerPartition: messagesPerPartition)
            default: sampleTopic(name: String(format: "topic-%05d", index), messagesPerPartition: 0, variant: index)
            }
        }
        seeds.sort { $0.detail.name < $1.detail.name }
        return FakeKafkaClient(topics: seeds, groups: topicCount >= 2 ? sampleGroups(end: Int64(messagesPerPartition)) : [])
    }

    /// `variant` makes some generated topics unhealthy or customised, deterministically: every 7th has an
    /// under-replicated partition, every 11th a leaderless one, every 5th overrides its retention.
    private static func sampleTopic(name: String, messagesPerPartition: Int, variant: Int = 0) -> SeedTopic {
        var partitions = (0..<samplePartitions).map {
            PartitionInfo(id: $0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3], startOffset: 0, endOffset: Int64(messagesPerPartition))
        }
        if variant % 7 == 3 {
            partitions[1] = PartitionInfo(id: 1, leader: 1, replicas: [1, 2, 3], isr: [1, 2], startOffset: 0, endOffset: Int64(messagesPerPartition))
        }
        if variant % 11 == 5 {
            partitions[2] = PartitionInfo(id: 2, leader: nil, replicas: [1, 2, 3], isr: [], startOffset: 0, endOffset: Int64(messagesPerPartition))
        }
        var entries = [
            ConfigEntry(name: "cleanup.policy", value: name == "orders" ? "compact" : "delete", isDefault: name != "orders"),
            ConfigEntry(name: "retention.ms", value: variant % 5 == 0 && variant > 1 ? "3600000" : "604800000", isDefault: !(variant % 5 == 0 && variant > 1)),
            ConfigEntry(name: "segment.bytes", value: "1073741824", isDefault: true),
        ]
        if name == "payments" {
            entries.append(ConfigEntry(name: "sample.credentials", value: "never-shown", isDefault: false, isSensitive: true))
        }
        let messages = (0..<samplePartitions).flatMap { partition in
            (0..<messagesPerPartition).map { offset in
                KafkaMessage(
                    partition: partition,
                    offset: Int64(offset),
                    timestamp: sampleEpoch.addingTimeInterval(Double(offset * samplePartitions + partition)),
                    key: offset % 17 == 0 && offset > 0 ? nil : Data("\(name)-\(partition)-\(offset)".utf8),
                    value: sampleValue(topic: name, partition: partition, offset: offset)
                )
            }
        }
        return SeedTopic(
            detail: TopicDetail(name: name, partitions: partitions, configEntries: entries),
            messages: messages
        )
    }

    /// A deterministic mix by offset: mostly JSON objects, then an array, plain text, binary, a tombstone and nested
    /// JSON. In `orders` partition 0, every 499th message carries a ~200 KB value.
    private static func sampleValue(topic: String, partition: Int, offset: Int) -> Data? {
        if topic == "orders", partition == 0, offset % 499 == 498 {
            return Data(#"{"blob":""#.utf8) + Data(repeating: UInt8(ascii: "x"), count: 200_000) + Data(#""}"#.utf8)
        }
        switch offset % 10 {
        case 5: return Data(#"[\#(offset),\#(partition),"\#(topic)"]"#.utf8)
        case 6: return Data("Plain text event \(offset) for \(topic)\nsecond line".utf8)
        case 7: return Data([0xFF, 0x00, 0x10, UInt8(truncatingIfNeeded: offset), 0x80, 0xC0])
        case 8: return nil
        case 9: return Data(#"{"topic":"\#(topic)","detail":{"partition":\#(partition),"tags":["a","b"],"offset":\#(offset)}}"#.utf8)
        default: return Data(#"{"topic":"\#(topic)","partition":\#(partition),"offset":\#(offset)}"#.utf8)
        }
    }

    private static func sampleGroups(end: Int64) -> [GroupDetail] {
        let member = GroupMemberInfo(id: "member-1", clientId: "sample-client", host: "/127.0.0.1", assignments: ["orders": [0, 1, 2]])
        return [
            GroupDetail(id: "orders-service", state: "Stable", members: [member], lags: [
                PartitionLag(topic: "orders", partition: 0, committedOffset: end - 5, startOffset: 0, endOffset: end),
                PartitionLag(topic: "orders", partition: 1, committedOffset: end, startOffset: 0, endOffset: end),
                PartitionLag(topic: "orders", partition: 2, committedOffset: nil, startOffset: 0, endOffset: end),
            ]),
            GroupDetail(id: "payments-service", state: "Empty", members: [], lags: [
                PartitionLag(topic: "payments", partition: 0, committedOffset: end + 20, startOffset: 0, endOffset: end),
            ]),
        ]
    }
}
