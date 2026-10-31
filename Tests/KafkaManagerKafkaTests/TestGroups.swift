import CRdKafka
import Foundation
@testable import KafkaManagerKafka

/// Test-only consumer that really joins a group. Used ONLY with groups named `km-itest-g-*` that the test owns.
final class TestGroupConsumer {
    private let client: RdClient

    init(bootstrap: String, group: String) throws {
        precondition(group.hasPrefix("km-itest-g-"), "tests may only join their own groups")
        let conf = RdConf()
        for (key, value) in [
            ("bootstrap.servers", bootstrap), ("group.id", group), ("enable.auto.commit", "false"),
            ("auto.offset.reset", "earliest"), ("allow.auto.create.topics", "false"),
        ] {
            if let problem = conf.set(key, value) { throw RdFailure.config(problem) }
        }
        client = try RdClient(conf: conf, sink: ErrorSink(), type: RD_KAFKA_CONSUMER)
    }

    /// Subscribes and polls until `count` partitions are assigned.
    func join(topic: String, expectingPartitions count: Int, timeout: Duration = .seconds(30)) throws {
        let topics = try RdPartitionList(capacity: 1)
        topics.add(topic: topic, partition: -1, offset: Int64(RD_KAFKA_OFFSET_INVALID))
        let code = rd_kafka_subscribe(client.pointer, topics.pointer)
        guard code == RD_KAFKA_RESP_ERR_NO_ERROR else { throw RdFailure.code(code.rawValue) }
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            if let raw = rd_kafka_consumer_poll(client.pointer, 200) { _ = RdMessage(raw) }
            var assignment: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?
            if rd_kafka_assignment(client.pointer, &assignment) == RD_KAFKA_RESP_ERR_NO_ERROR, let assignment {
                let assigned = Int(assignment.pointee.cnt)
                rd_kafka_topic_partition_list_destroy(assignment)
                if assigned == count { return }
            }
        }
        throw RdFailure.code(RD_KAFKA_RESP_ERR__TIMED_OUT.rawValue)
    }

    func commit(topic: String, offsets: [Int: Int64]) throws {
        let list = try RdPartitionList(capacity: offsets.count)
        for (partition, offset) in offsets { list.add(topic: topic, partition: partition, offset: offset) }
        let code = rd_kafka_commit(client.pointer, list.pointer, 0)
        guard code == RD_KAFKA_RESP_ERR_NO_ERROR else { throw RdFailure.code(code.rawValue) }
    }

    /// Leaves the group (the group becomes Empty) and destroys the consumer.
    func close() { client.close() }
}

/// Deletes a test-owned group. Best effort: a group that doesn't exist or isn't empty is left alone.
func deleteTestGroup(bootstrap: String, group: String) {
    guard group.hasPrefix("km-itest-g-") else { return }
    let conf = RdConf()
    _ = conf.set("bootstrap.servers", bootstrap)
    guard let client = try? RdClient(conf: conf, sink: ErrorSink()),
          let options = try? RdAdminOptions(client: client, operation: RD_KAFKA_ADMIN_OP_DELETEGROUPS, timeoutMs: 10_000),
          let queue = try? RdQueue(client: client),
          let pointer = rd_kafka_DeleteGroup_new(group) else { return }
    let request = RdAdminObject(pointer) { rd_kafka_DeleteGroup_destroy($0) }
    var requests: [OpaquePointer?] = [request.pointer]
    requests.withUnsafeMutableBufferPointer { rd_kafka_DeleteGroups(client.pointer, $0.baseAddress, 1, options.pointer, queue.pointer) }
    _ = queue.poll(timeoutMs: 11_000)
}

/// Every offset reset the tests run goes through this: it refuses any group that is not one the test created itself.
func requireTestOwnedGroup(_ group: String) throws {
    guard group.hasPrefix("km-itest-g-"), group.count > "km-itest-g-".count else { throw RdFailure.config("not a test-owned group: \(group)") }
}
