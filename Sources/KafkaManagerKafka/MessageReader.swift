import CRdKafka
import Foundation
import KafkaManagerCore

/// What a fetch needs to open its own consumer. Holds the password like the connection settings do.
struct ConsumerSettings: Sendable {
    let properties: [(key: String, value: String)]
    let secrets: [String]
    let host: String
}

/// Reads one `MessageQuery` with a consumer of its own, to the end of the data or the limit.
///
/// The consumer only ever `assign`s partitions with explicit offsets and never commits: no JoinGroup, no OffsetCommit,
/// so no group's offsets can change. It is created for the fetch and closed when the fetch ends (memory goes back to
/// the baseline, see the spike). Blocking; the caller runs it on a thread of its own.
enum MessageReader {
    /// Tuned buffers from the T04 spike: the defaults prefetch ~80 MB that is never returned.
    static let tuning: [(key: String, value: String)] = [
        ("queued.max.messages.kbytes", "1024"),
        ("queued.min.messages", "100"),
        ("fetch.max.bytes", "1048576"),
        ("fetch.message.max.bytes", "262144"),
        ("fetch.wait.max.ms", "50"),
        ("fetch.queue.backoff.ms", "50"),
    ]

    /// Poll slice, so a stop request is noticed within this time.
    static let pollSliceMs: Int32 = 100

    /// - Parameters:
    ///   - isStopped: checked between polls.
    ///   - emit: receives each message; returns false to stop (limit reached by the consumer of the stream, or termination).
    /// - Throws: `KafkaError`, already mapped.
    static func run(
        settings: ConsumerSettings, query: MessageQuery, timeoutMs: Int32,
        isStopped: () -> Bool, emit: (KafkaMessage) -> Bool
    ) throws {
        guard query.limit > 0 else { return }
        let sink = ErrorSink()
        do {
            try read(settings: settings, query: query, timeoutMs: timeoutMs, sink: sink, isStopped: isStopped, emit: emit)
        } catch {
            throw LiveKafkaClient.map(error, sink: sink, host: settings.host, secrets: settings.secrets, resource: query.topic)
        }
    }

    private static func read(
        settings: ConsumerSettings, query: MessageQuery, timeoutMs: Int32, sink: ErrorSink,
        isStopped: () -> Bool, emit: (KafkaMessage) -> Bool
    ) throws {
        let properties = settings.properties + [
            // A group id is mandatory for assign(); nothing ever joins or commits under it.
            ("group.id", "km-noop-\(UUID().uuidString.prefix(8).lowercased())"),
            ("enable.auto.commit", "false"),
            ("enable.auto.offset.store", "false"),
            ("enable.partition.eof", "true"),
            ("allow.auto.create.topics", "false"),
        ] + tuning
        let client = try LiveKafkaClient.makeClient(properties: properties, secrets: settings.secrets, sink: sink, type: RD_KAFKA_CONSUMER)
        defer { client.close() }

        // Metadata first: assigning to a partition with an unknown leader costs a fixed ~500 ms before the first message.
        let topic = try RdTopic(client: client, name: query.topic)
        let metadata = try RdMetadata(client: client, allTopics: false, topic: topic, timeoutMs: timeoutMs)
        let known = try LiveKafkaClient.partitions(of: query.topic, in: metadata)
        var wanted = known.filter { $0.leader != nil }.map(\.id)
        if let requested = query.partition {
            guard known.contains(where: { $0.id == requested }) else {
                throw KafkaError.clusterError(code: Int(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION.rawValue), message: "unknown partition")
            }
            wanted = wanted.filter { $0 == requested }
        }

        var marks: [Int: (low: Int64, high: Int64)] = [:]
        for partition in wanted {
            marks[partition] = try LiveKafkaClient.queryWatermarks(client: client, topic: query.topic, partition: partition, timeoutMs: timeoutMs)
        }
        let starts = try startOffsets(query: query, partitions: wanted, marks: marks, client: client, timeoutMs: timeoutMs)
        // Partitions with nothing to read are not assigned; if none is left the stream just ends.
        var remaining = starts.filter { start in start.value < (marks[start.key]?.high ?? 0) }
        guard !remaining.isEmpty else { return }

        let assignment = try RdPartitionList(capacity: remaining.count)
        for (partition, offset) in remaining.sorted(by: { $0.key < $1.key }) {
            assignment.add(topic: query.topic, partition: partition, offset: offset)
        }
        let assigned = rd_kafka_assign(client.pointer, assignment.pointer)
        guard assigned == RD_KAFKA_RESP_ERR_NO_ERROR else { throw RdFailure.code(assigned.rawValue) }

        // latest(N) over several partitions: keep the N newest by timestamp, then deliver them in partition/offset order.
        var tail: Tail?
        if case .latest(let count) = query.start, remaining.count > 1 { tail = Tail(capacity: max(0, min(count, query.limit))) }
        var delivered = 0
        var lastProgress = ContinuousClock.now
        let idleLimit = Duration.milliseconds(Int(timeoutMs))

        loop: while !remaining.isEmpty {
            if isStopped() { return }
            guard let raw = rd_kafka_consumer_poll(client.pointer, pollSliceMs) else {
                if ContinuousClock.now - lastProgress > idleLimit { throw RdFailure.code(RD_KAFKA_RESP_ERR__TIMED_OUT.rawValue) }
                continue
            }
            let polled = RdMessage(raw)
            lastProgress = ContinuousClock.now
            let partition = Int(raw.pointee.partition)
            let code = raw.pointee.err
            if code == RD_KAFKA_RESP_ERR__PARTITION_EOF {
                remaining.removeValue(forKey: partition)
                continue
            }
            guard code == RD_KAFKA_RESP_ERR_NO_ERROR else { throw RdFailure.code(code.rawValue) }
            guard let high = marks[partition]?.high, remaining[partition] != nil else { continue }
            let message = makeMessage(polled)
            if message.offset >= high - 1 { remaining.removeValue(forKey: partition) }
            if tail != nil {
                tail?.add(message)
            } else {
                guard emit(message) else { return }
                delivered += 1
                if delivered >= query.limit { break loop }
            }
        }
        if let tail {
            for message in tail.sorted { if !emit(message) { return } }
        }
    }

    private static func startOffsets(
        query: MessageQuery, partitions: [Int], marks: [Int: (low: Int64, high: Int64)], client: RdClient, timeoutMs: Int32
    ) throws -> [Int: Int64] {
        var result: [Int: Int64] = [:]
        switch query.start {
        case .earliest:
            for partition in partitions { result[partition] = marks[partition]?.low }
        case .latest(let count):
            let count = Int64(max(0, count))
            for partition in partitions {
                guard let mark = marks[partition] else { continue }
                result[partition] = max(mark.low, mark.high - count)
            }
        case .offset(let offset):
            for partition in partitions {
                guard let mark = marks[partition] else { continue }
                result[partition] = min(max(offset, mark.low), mark.high)
            }
        case .timestamp(let date):
            let list = try RdPartitionList(capacity: partitions.count)
            let millis = Int64((date.timeIntervalSince1970 * 1000).rounded(.down))
            for partition in partitions { list.add(topic: query.topic, partition: partition, offset: millis) }
            let code = rd_kafka_offsets_for_times(client.pointer, list.pointer, timeoutMs)
            guard code == RD_KAFKA_RESP_ERR_NO_ERROR else { throw RdFailure.code(code.rawValue) }
            for element in list.elements {
                guard let mark = marks[element.partition] else { continue }
                if element.error != RD_KAFKA_RESP_ERR_NO_ERROR.rawValue { throw RdFailure.code(element.error) }
                // No message at or after the time: the end of the log, which means nothing to read.
                result[element.partition] = element.offset < 0 ? mark.high : min(max(element.offset, mark.low), mark.high)
            }
        }
        return result
    }

    /// Key and value keep the difference between "absent" (nil, a tombstone for the value) and "empty".
    private static func makeMessage(_ polled: RdMessage) -> KafkaMessage {
        let raw = polled.pointer.pointee
        let key = raw.key.map { Data(bytes: $0, count: raw.key_len) }
        let value = raw.payload.map { Data(bytes: $0, count: raw.len) }
        var timestampKind = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE
        let millis = rd_kafka_message_timestamp(polled.pointer, &timestampKind)
        let type: TimestampType
        switch timestampKind {
        case RD_KAFKA_TIMESTAMP_CREATE_TIME: type = .createTime
        case RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME: type = .logAppendTime
        default: type = .notAvailable
        }
        // Without a timestamp librdkafka reports -1; the epoch is shown instead and `timestampType` says why.
        let date = type == .notAvailable ? Date(timeIntervalSince1970: 0) : Date(timeIntervalSince1970: Double(millis) / 1000)
        return KafkaMessage(
            partition: Int(raw.partition), offset: raw.offset, timestamp: date, key: key, value: value,
            headers: headers(of: polled), timestampType: type)
    }

    private static func headers(of polled: RdMessage) -> [MessageHeader] {
        var list: OpaquePointer?
        guard rd_kafka_message_headers(polled.pointer, &list) == RD_KAFKA_RESP_ERR_NO_ERROR, let list else { return [] }
        return (0..<rd_kafka_header_cnt(list)).compactMap { index in
            var name: UnsafePointer<CChar>?
            var value: UnsafeRawPointer?
            var size = 0
            guard rd_kafka_header_get_all(list, index, &name, &value, &size) == RD_KAFKA_RESP_ERR_NO_ERROR, let name else { return nil }
            return MessageHeader(key: String(cString: name), value: value.map { Data(bytes: $0, count: size) })
        }
    }

    /// The N newest messages seen so far, trimmed in batches so memory stays near N.
    private struct Tail {
        let capacity: Int
        private var messages: [KafkaMessage] = []

        init(capacity: Int) { self.capacity = capacity }

        mutating func add(_ message: KafkaMessage) {
            messages.append(message)
            if messages.count > capacity * 2 + 16 { trim() }
        }

        private mutating func trim() {
            messages.sort { ($0.timestamp, $0.offset, $0.partition) > ($1.timestamp, $1.offset, $1.partition) }
            if messages.count > capacity { messages.removeLast(messages.count - capacity) }
        }

        /// In partition and offset order, like the fake client.
        var sorted: [KafkaMessage] {
            var copy = self
            copy.trim()
            return copy.messages.sorted { ($0.partition, $0.offset) < ($1.partition, $1.offset) }
        }
    }
}
