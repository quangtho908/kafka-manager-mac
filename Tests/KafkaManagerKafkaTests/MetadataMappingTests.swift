import CRdKafka
import Foundation
import KafkaManagerCore
import Testing
@testable import KafkaManagerKafka

/// The topic mapping on hand-built C metadata: no broker, no client.
@Suite struct MetadataMappingTests {
    /// Owns the C memory of a synthetic metadata response and frees it.
    private final class Synthetic {
        var topics: UnsafeMutablePointer<rd_kafka_metadata_topic>
        let count: Int
        private var partitions: [UnsafeMutablePointer<rd_kafka_metadata_partition>] = []
        private var names: [UnsafeMutablePointer<CChar>] = []

        /// Topic `i` has `1 + i % 3` partitions; partition 0 has `1 + i % 3` replicas.
        init(count: Int) {
            self.count = count
            topics = .allocate(capacity: max(count, 1))
            for index in 0..<count {
                let partitionCount = 1 + index % 3
                let list = UnsafeMutablePointer<rd_kafka_metadata_partition>.allocate(capacity: partitionCount)
                for partition in 0..<partitionCount {
                    list[partition] = rd_kafka_metadata_partition(
                        id: Int32(partition), err: RD_KAFKA_RESP_ERR_NO_ERROR, leader: 1,
                        replica_cnt: Int32(partition == 0 ? 1 + index % 3 : 1), replicas: nil, isr_cnt: 0, isrs: nil)
                }
                partitions.append(list)
                let name = strdup(String(format: "topic-%05d", index))!
                names.append(name)
                (topics + index).initialize(to: rd_kafka_metadata_topic(
                    topic: name, partition_cnt: Int32(partitionCount), partitions: list, err: RD_KAFKA_RESP_ERR_NO_ERROR))
            }
        }

        deinit {
            partitions.forEach { $0.deallocate() }
            names.forEach { free($0) }
            topics.deallocate()
        }

        var metadata: rd_kafka_metadata {
            rd_kafka_metadata(broker_cnt: 0, brokers: nil, topic_cnt: Int32(count), topics: topics, orig_broker_id: 1, orig_broker_name: nil)
        }
    }

    @Test func smallMetadataMapsNamesPartitionsAndReplication() {
        let synthetic = Synthetic(count: 7)
        let result = LiveKafkaClient.topics(in: synthetic.metadata)
        #expect(result.count == 7)
        #expect(result[4].name == "topic-00004")
        #expect(result[4].partitionCount == 2)
        #expect(result[4].replicationFactor == 2)
        #expect(result[2].partitionCount == 3 && result[2].replicationFactor == 3)
    }

    @Test func emptyMetadataIsAnEmptyList() {
        #expect(LiveKafkaClient.topics(in: Synthetic(count: 0).metadata).isEmpty)
    }

    @Test func fiveThousandTopicsMapFastAndStayInBudget() {
        let synthetic = Synthetic(count: 5000)
        let metadata = synthetic.metadata
        let before = Self.residentBytes()
        var result: [TopicSummary] = []
        let time = ContinuousClock().measure { result = LiveKafkaClient.topics(in: metadata) }
        let delta = Self.residentBytes() - before
        #expect(result.count == 5000)
        #expect(result.last?.name == "topic-04999")
        #expect(time < .milliseconds(100), "mapping took \(time)")
        // Names and three Ints per topic: far below a megabyte per thousand topics.
        #expect(delta < 20_000_000, "resident grew by \(delta) bytes")
        Swift.print("T61 mapping 5000 topics: \(time), resident delta \(delta) bytes")
    }

    static func residentBytes() -> Int {
        var info = mach_task_basic_info()
        var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size / MemoryLayout<natural_t>.size)
        let result = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) { task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), $0, &count) }
        }
        return result == KERN_SUCCESS ? Int(info.resident_size) : 0
    }
}
