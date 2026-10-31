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
            default: sampleTopic(name: String(format: "topic-%05d", index), messagesPerPartition: 0)
            }
        }
        seeds.sort { $0.detail.name < $1.detail.name }
        return FakeKafkaClient(topics: seeds, groups: topicCount >= 2 ? sampleGroups() : [])
    }

    private static func sampleTopic(name: String, messagesPerPartition: Int) -> SeedTopic {
        let partitions = (0..<samplePartitions).map {
            PartitionInfo(id: $0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3], startOffset: 0, endOffset: Int64(messagesPerPartition))
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
            detail: TopicDetail(name: name, partitions: partitions, config: ["retention.ms": "604800000"]),
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
