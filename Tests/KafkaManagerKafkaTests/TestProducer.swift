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

    private func produce(_ message: TestMessage, to topic: String) throws {
        var allocations: [UnsafeMutableRawPointer] = []
        defer { allocations.forEach { $0.deallocate() } }
        func copy(_ data: Data) -> UnsafeMutableRawPointer {
            // At least one byte, so an empty value still has a non-null pointer.
            let pointer = UnsafeMutableRawPointer.allocate(byteCount: max(data.count, 1), alignment: 1)
            data.copyBytes(to: pointer.assumingMemoryBound(to: UInt8.self), count: data.count)
            allocations.append(pointer)
            return pointer
        }
        func cString(_ text: String) -> UnsafePointer<CChar> {
            let bytes = Array(text.utf8CString)
            let pointer = UnsafeMutablePointer<CChar>.allocate(capacity: bytes.count)
            pointer.initialize(from: bytes, count: bytes.count)
            allocations.append(UnsafeMutableRawPointer(pointer))
            return UnsafePointer(pointer)
        }
        var vus: [rd_kafka_vu_t] = []
        var topicVU = rd_kafka_vu_t(); topicVU.vtype = RD_KAFKA_VTYPE_TOPIC
        topicVU.u.cstr = cString(topic); vus.append(topicVU)
        var partitionVU = rd_kafka_vu_t(); partitionVU.vtype = RD_KAFKA_VTYPE_PARTITION
        partitionVU.u.i32 = message.partition; vus.append(partitionVU)
        var flagsVU = rd_kafka_vu_t(); flagsVU.vtype = RD_KAFKA_VTYPE_MSGFLAGS
        flagsVU.u.i = RD_KAFKA_MSG_F_COPY; vus.append(flagsVU)
        var valueVU = rd_kafka_vu_t(); valueVU.vtype = RD_KAFKA_VTYPE_VALUE
        if let value = message.value { valueVU.u.mem.ptr = copy(value); valueVU.u.mem.size = value.count }
        vus.append(valueVU)
        var keyVU = rd_kafka_vu_t(); keyVU.vtype = RD_KAFKA_VTYPE_KEY
        if let key = message.key { keyVU.u.mem.ptr = copy(key); keyVU.u.mem.size = key.count }
        vus.append(keyVU)
        if let timestamp = message.timestampMs {
            var timestampVU = rd_kafka_vu_t(); timestampVU.vtype = RD_KAFKA_VTYPE_TIMESTAMP
            timestampVU.u.i64 = timestamp; vus.append(timestampVU)
        }
        for header in message.headers {
            var headerVU = rd_kafka_vu_t(); headerVU.vtype = RD_KAFKA_VTYPE_HEADER
            headerVU.u.header.name = cString(header.name)
            if let value = header.value {
                headerVU.u.header.val = UnsafeRawPointer(copy(value)); headerVU.u.header.size = value.count
            } else {
                headerVU.u.header.val = nil; headerVU.u.header.size = 0
            }
            vus.append(headerVU)
        }
        if let error = rd_kafka_produceva(client.pointer, vus, vus.count) {
            let code = rd_kafka_error_code(error)
            rd_kafka_error_destroy(error)
            throw RdFailure.code(code.rawValue)
        }
    }
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
