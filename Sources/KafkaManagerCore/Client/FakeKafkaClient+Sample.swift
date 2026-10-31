import Foundation

extension FakeKafkaClient {
    private static let sampleEpoch = Date(timeIntervalSince1970: 1_700_000_000)
    private static let samplePartitions = 3
    private static let sampleMessagesPerPartition = 30

    /// Deterministic dataset for previews and perf checks. The first two topics (`orders`, `payments`)
    /// carry messages and consumer groups: one partition with no commit, one committed past the log end.
    public static func sampleData(topicCount: Int = 5000) -> FakeKafkaClient {
        var seeds: [SeedTopic] = (0..<topicCount).map { index in
            switch index {
            case 0: sampleTopic(name: "orders", messagesPerPartition: sampleMessagesPerPartition)
            case 1: sampleTopic(name: "payments", messagesPerPartition: sampleMessagesPerPartition)
            default: sampleTopic(name: String(format: "topic-%05d", index), messagesPerPartition: 0, variant: index)
            }
        }
        seeds.sort { $0.detail.name < $1.detail.name }
        return FakeKafkaClient(topics: seeds, groups: topicCount >= 2 ? sampleGroups() : [])
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
                    key: Data("\(name)-\(partition)-\(offset)".utf8),
                    value: Data(#"{"topic":"\#(name)","partition":\#(partition),"offset":\#(offset)}"#.utf8)
                )
            }
        }
        return SeedTopic(
            detail: TopicDetail(name: name, partitions: partitions, configEntries: entries),
            messages: messages
        )
    }

    private static func sampleGroups() -> [GroupDetail] {
        let end = Int64(sampleMessagesPerPartition)
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
