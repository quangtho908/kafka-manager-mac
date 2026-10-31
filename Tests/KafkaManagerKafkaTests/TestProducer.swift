import CRdKafka
import Foundation
@testable import KafkaManagerKafka

/// Test-only producer for the `km-itest-*` topics. `rd_kafka_producev` is variadic and not importable, so this uses
/// `rd_kafka_produceva`.
struct TestMessage {
    var partition: Int32
    var key: Data?
    var value: Data?
    var headers: [(name: String, value: Data?)] = []
    var timestampMs: Int64?
}

final class TestProducer {
    private let client: RdClient

    init(bootstrap: String) throws {
        let conf = RdConf()
        _ = conf.set("bootstrap.servers", bootstrap)
        _ = conf.set("allow.auto.create.topics", "false")
        _ = conf.set("message.max.bytes", "2097152")
        client = try RdClient(conf: conf, sink: ErrorSink())
    }

    func produce(_ messages: [TestMessage], to topic: String) throws {
        for message in messages { try produce(message, to: topic) }
        let remaining = rd_kafka_flush(client.pointer, 20_000)
        guard remaining == RD_KAFKA_RESP_ERR_NO_ERROR else { throw RdFailure.code(remaining.rawValue) }
    }

    /// Only `km-itest-*` topics: the helper refuses anything else before a byte is queued.
    private func produce(_ message: TestMessage, to topic: String) throws {
        try requireTestOwnedTopic(topic)
        try RdProduce.enqueue(
            client: client, topic: topic, partition: message.partition, key: message.key, value: message.value,
            headers: message.headers.map { RdProduce.Header(name: $0.name, value: $0.value) },
            timestampMs: message.timestampMs, opaque: nil)
    }
}

/// Every test produce goes through this: it refuses any topic that is not one the test created itself.
func requireTestOwnedTopic(_ topic: String) throws {
    guard topic.hasPrefix("km-itest-"), topic.count > "km-itest-".count else { throw RdFailure.config("not a test-owned topic: \(topic)") }
}

/// Existing consumer groups as (name, state, member count): a snapshot to prove a read changed nothing.
func groupSnapshot(bootstrap: String) throws -> [String] {
    let conf = RdConf()
    _ = conf.set("bootstrap.servers", bootstrap)
    let client = try RdClient(conf: conf, sink: ErrorSink())
    var list: UnsafePointer<rd_kafka_group_list>?
    let code = rd_kafka_list_groups(client.pointer, nil, &list, 10_000)
    guard code == RD_KAFKA_RESP_ERR_NO_ERROR, let list else { throw RdFailure.code(code.rawValue) }
    defer { rd_kafka_group_list_destroy(list) }
    return (0..<Int(list.pointee.group_cnt)).map { index in
        let group = list.pointee.groups[index]
        return "\(String(cString: group.group)) \(String(cString: group.state)) \(group.member_cnt)"
    }.sorted()
}
