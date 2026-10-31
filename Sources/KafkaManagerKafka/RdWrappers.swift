import os
import CRdKafka
import Foundation

// RAII owners for librdkafka objects. Each frees its object exactly once, in deinit, and is counted in
// `ResourceCounters`. They are used only inside `LiveKafkaClient` (one executor), so none is Sendable.

/// Size of the error string buffers handed to librdkafka.
let errorBufferSize = 512

func withErrorBuffer<T>(_ body: (UnsafeMutablePointer<CChar>, Int) -> T) -> (T, String) {
    let buffer = UnsafeMutablePointer<CChar>.allocate(capacity: errorBufferSize)
    buffer.initialize(repeating: 0, count: errorBufferSize)
    defer { buffer.deallocate() }
    let result = body(buffer, errorBufferSize)
    return (result, String(cString: buffer))
}

final class RdConf {
    private var pointer: OpaquePointer?

    init() {
        pointer = rd_kafka_conf_new()
        ResourceCounters.acquired(.conf)
    }

    deinit {
        if let pointer { rd_kafka_conf_destroy(pointer) }
        ResourceCounters.released(.conf)
    }

    /// Returns librdkafka's complaint on failure.
    func set(_ key: String, _ value: String) -> String? {
        guard let pointer else { return "configuration already used" }
        let (result, message) = withErrorBuffer { rd_kafka_conf_set(pointer, key, value, $0, $1) }
        return result == RD_KAFKA_CONF_OK ? nil : message
    }

    func setLogCallback(_ callback: @convention(c) (OpaquePointer?, Int32, UnsafePointer<CChar>?, UnsafePointer<CChar>?) -> Void) {
        if let pointer { rd_kafka_conf_set_log_cb(pointer, callback) }
    }

    func setErrorCallback(_ callback: @convention(c) (OpaquePointer?, Int32, UnsafePointer<CChar>?, UnsafeMutableRawPointer?) -> Void) {
        if let pointer { rd_kafka_conf_set_error_cb(pointer, callback) }
    }

    /// Called from `rd_kafka_poll` (on the thread that polls) once per produced message, with its per-message opaque in
    /// `_private`.
    func setDeliveryCallback(_ callback: @convention(c) (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, UnsafeMutableRawPointer?) -> Void) {
        if let pointer { rd_kafka_conf_set_dr_msg_cb(pointer, callback) }
    }

    func setOpaque(_ value: UnsafeMutableRawPointer?) {
        if let pointer { rd_kafka_conf_set_opaque(pointer, value) }
    }

    /// Hands the configuration to a new client. On success librdkafka owns it; on failure it is still ours.
    fileprivate func makeClient(type: rd_kafka_type_t) -> (OpaquePointer?, String) {
        guard let conf = pointer else { return (nil, "configuration already used") }
        let (client, message) = withErrorBuffer { rd_kafka_new(type, conf, $0, $1) }
        if client != nil { pointer = nil }
        return (client, message)
    }
}

/// Fired by librdkafka's background thread. Must be thread-safe; see `ErrorSink`.
final class RdClient {
    let pointer: OpaquePointer
    private let sink: Unmanaged<ErrorSink>
    private let isConsumer: Bool
    private var isClosed = false

    /// Takes the configuration; `sink` receives the client's asynchronous errors and is released with the client.
    init(conf: RdConf, sink: ErrorSink, type: rd_kafka_type_t = RD_KAFKA_PRODUCER) throws {
        let retained = Unmanaged.passRetained(sink)
        conf.setOpaque(retained.toOpaque())
        let (client, message) = conf.makeClient(type: type)
        guard let client else {
            retained.release()
            throw RdFailure.config(message)
        }
        pointer = client
        self.sink = retained
        isConsumer = type == RD_KAFKA_CONSUMER
        ResourceCounters.acquired(.client)
    }

    /// A consumer leaves its group state cleanly before it is destroyed. Safe to call twice.
    func close() {
        guard isConsumer, !isClosed else { return }
        isClosed = true
        rd_kafka_consumer_close(pointer)
    }

    deinit {
        close()
        rd_kafka_destroy(pointer)
        sink.release()
        ResourceCounters.released(.client)
    }
}

final class RdMetadata {
    let pointer: UnsafePointer<rd_kafka_metadata>

    /// Wraps a metadata call: `topic` limits it to one topic, nil with `allTopics` false returns brokers only.
    init(client: RdClient, allTopics: Bool, topic: RdTopic?, timeoutMs: Int32) throws {
        var result: UnsafePointer<rd_kafka_metadata>?
        let code = rd_kafka_metadata(client.pointer, allTopics ? 1 : 0, topic?.pointer, &result, timeoutMs)
        guard code == RD_KAFKA_RESP_ERR_NO_ERROR, let result else { throw RdFailure.code(code.rawValue) }
        pointer = result
        ResourceCounters.acquired(.metadata)
    }

    deinit {
        rd_kafka_metadata_destroy(pointer)
        ResourceCounters.released(.metadata)
    }
}

final class RdTopic {
    let pointer: OpaquePointer
    /// Keeps the client alive until the topic object is released; librdkafka needs that order.
    private let owner: RdClient

    init(client: RdClient, name: String) throws {
        owner = client
        guard let topic = rd_kafka_topic_new(client.pointer, name, nil) else {
            throw RdFailure.code(rd_kafka_last_error().rawValue)
        }
        pointer = topic
        ResourceCounters.acquired(.topic)
    }

    deinit {
        rd_kafka_topic_destroy(pointer)
        ResourceCounters.released(.topic)
    }
}

final class RdQueue {
    let pointer: OpaquePointer
    private let owner: RdClient

    init(client: RdClient) throws {
        owner = client
        guard let queue = rd_kafka_queue_new(client.pointer) else { throw RdFailure.code(rd_kafka_last_error().rawValue) }
        pointer = queue
        ResourceCounters.acquired(.queue)
    }

    deinit {
        rd_kafka_queue_destroy(pointer)
        ResourceCounters.released(.queue)
    }

    /// Waits for one event. nil on timeout.
    func poll(timeoutMs: Int32) -> RdEvent? {
        guard let event = rd_kafka_queue_poll(pointer, timeoutMs) else { return nil }
        return RdEvent(event)
    }
}

final class RdEvent {
    let pointer: OpaquePointer

    fileprivate init(_ pointer: OpaquePointer) {
        self.pointer = pointer
        ResourceCounters.acquired(.event)
    }

    deinit {
        rd_kafka_event_destroy(pointer)
        ResourceCounters.released(.event)
    }

    var type: Int32 { rd_kafka_event_type(pointer) }

    /// The operation-level error, if any.
    var errorCode: Int32 { rd_kafka_event_error(pointer).rawValue }
}

final class RdAdminOptions {
    let pointer: OpaquePointer

    init(client: RdClient, operation: rd_kafka_admin_op_t, timeoutMs: Int32) throws {
        guard let options = rd_kafka_AdminOptions_new(client.pointer, operation) else { throw RdFailure.code(rd_kafka_last_error().rawValue) }
        pointer = options
        ResourceCounters.acquired(.adminOptions)
        let (code, message) = withErrorBuffer { rd_kafka_AdminOptions_set_request_timeout(options, timeoutMs, $0, $1) }
        guard code == RD_KAFKA_RESP_ERR_NO_ERROR else { throw RdFailure.config(message) }
        let (code2, message2) = withErrorBuffer { rd_kafka_AdminOptions_set_operation_timeout(options, timeoutMs, $0, $1) }
        // Only create/delete topics accept an operation timeout; other operations answer "not supported", which is fine.
        _ = (code2, message2)
    }

    deinit {
        rd_kafka_AdminOptions_destroy(pointer)
        ResourceCounters.released(.adminOptions)
    }
}

/// An admin request object (NewTopic, DeleteTopic, ConfigResource) with the matching destroy.
final class RdAdminObject {
    let pointer: OpaquePointer
    private let destroy: (OpaquePointer) -> Void

    init(_ pointer: OpaquePointer, destroy: @escaping (OpaquePointer) -> Void) {
        self.pointer = pointer
        self.destroy = destroy
        ResourceCounters.acquired(.adminObject)
    }

    deinit {
        destroy(pointer)
        ResourceCounters.released(.adminObject)
    }
}

enum RdFailure: Error {
    case code(Int32)
    case config(String)
}

/// Collects what librdkafka reports from its own threads (broker down, auth failure), so a timed-out call can be
/// explained by the real cause. Thread-safe by construction: only a lock-protected value.
final class ErrorSink: Sendable {
    private let state = OSAllocatedUnfairLock(initialState: Int32?.none)

    func record(code: Int32) { state.withLock { $0 = code } }
    var last: Int32? { state.withLock { $0 } }
    func clear() { state.withLock { $0 = nil } }
}

final class RdPartitionList {
    let pointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>

    init(capacity: Int) throws {
        guard let list = rd_kafka_topic_partition_list_new(Int32(capacity)) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__INVALID_ARG.rawValue) }
        pointer = list
        ResourceCounters.acquired(.partitionList)
    }

    deinit {
        rd_kafka_topic_partition_list_destroy(pointer)
        ResourceCounters.released(.partitionList)
    }

    func add(topic: String, partition: Int, offset: Int64) {
        rd_kafka_topic_partition_list_add(pointer, topic, Int32(partition))?.pointee.offset = offset
    }

    var elements: [(partition: Int, offset: Int64, error: Int32)] {
        (0..<Int(pointer.pointee.cnt)).map { index in
            let element = pointer.pointee.elems[index]
            return (Int(element.partition), element.offset, element.err.rawValue)
        }
    }
}

/// One result of `rd_kafka_consumer_poll`; freed with the wrapper.
final class RdMessage {
    let pointer: UnsafeMutablePointer<rd_kafka_message_t>

    init(_ pointer: UnsafeMutablePointer<rd_kafka_message_t>) {
        self.pointer = pointer
        ResourceCounters.acquired(.message)
    }

    deinit {
        rd_kafka_message_destroy(pointer)
        ResourceCounters.released(.message)
    }
}
