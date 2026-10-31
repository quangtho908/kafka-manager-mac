import CRdKafka
import Foundation

/// Hands one message to a producer handle. `rd_kafka_producev` is variadic and not importable, so this uses
/// `rd_kafka_produceva`. The payload is copied (`RD_KAFKA_MSG_F_COPY`), so nothing here has to outlive the call.
enum RdProduce {
    struct Header {
        let name: String
        let value: Data?
    }

    /// `partition` -1 means the partitioner chooses. `opaque` comes back in the delivery report's `_private`.
    static func enqueue(
        client: RdClient, topic: String, partition: Int32, key: Data?, value: Data?, headers: [Header],
        timestampMs: Int64?, opaque: UnsafeMutableRawPointer?
    ) throws {
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
        partitionVU.u.i32 = partition; vus.append(partitionVU)
        var flagsVU = rd_kafka_vu_t(); flagsVU.vtype = RD_KAFKA_VTYPE_MSGFLAGS
        flagsVU.u.i = RD_KAFKA_MSG_F_COPY; vus.append(flagsVU)
        var valueVU = rd_kafka_vu_t(); valueVU.vtype = RD_KAFKA_VTYPE_VALUE
        if let value { valueVU.u.mem.ptr = copy(value); valueVU.u.mem.size = value.count }
        vus.append(valueVU)
        var keyVU = rd_kafka_vu_t(); keyVU.vtype = RD_KAFKA_VTYPE_KEY
        if let key { keyVU.u.mem.ptr = copy(key); keyVU.u.mem.size = key.count }
        vus.append(keyVU)
        if let timestampMs {
            var timestampVU = rd_kafka_vu_t(); timestampVU.vtype = RD_KAFKA_VTYPE_TIMESTAMP
            timestampVU.u.i64 = timestampMs; vus.append(timestampVU)
        }
        if let opaque {
            var opaqueVU = rd_kafka_vu_t(); opaqueVU.vtype = RD_KAFKA_VTYPE_OPAQUE
            opaqueVU.u.ptr = opaque; vus.append(opaqueVU)
        }
        for header in headers {
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

/// The delivery report of one message, filled in by the delivery callback while the producer is polled.
final class DeliveryBox {
    enum Outcome {
        case delivered(partition: Int, offset: Int64, timestampMs: Int64)
        case failed(code: Int32)
    }

    var outcome: Outcome?

    /// The callback fires on whichever thread polls, which is always the client's executor; it releases the retain
    /// taken when the box was handed to librdkafka.
    static let callback: @convention(c) (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, UnsafeMutableRawPointer?) -> Void = { _, message, _ in
        guard let message, let raw = message.pointee._private else { return }
        let box = Unmanaged<DeliveryBox>.fromOpaque(raw).takeRetainedValue()
        if message.pointee.err == RD_KAFKA_RESP_ERR_NO_ERROR {
            var type = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE
            let timestamp = rd_kafka_message_timestamp(message, &type)
            box.outcome = .delivered(partition: Int(message.pointee.partition), offset: message.pointee.offset, timestampMs: timestamp)
        } else {
            box.outcome = .failed(code: message.pointee.err.rawValue)
        }
    }
}

/// A producer handle with its error sink. Created on the first send and closed when idle or on disconnect.
final class ProducerHandle {
    let client: RdClient
    let sink: ErrorSink

    init(client: RdClient, sink: ErrorSink) {
        self.client = client
        self.sink = sink
    }

    /// Fails anything still queued and lets the delivery callbacks release their boxes. Call before dropping the handle.
    func drain() {
        _ = rd_kafka_purge(client.pointer, Int32(RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT))
        _ = rd_kafka_poll(client.pointer, 0)
    }
}
