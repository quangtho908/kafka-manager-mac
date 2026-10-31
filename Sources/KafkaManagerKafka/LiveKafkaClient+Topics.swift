import CRdKafka
import Foundation
import KafkaManagerCore

// Metadata parsing and the admin requests behind the topic operations.
extension LiveKafkaClient {
    static func topics(in metadata: RdMetadata) -> [TopicSummary] {
        let md = metadata.pointer.pointee
        return (0..<Int(md.topic_cnt)).map { index in
            let topic = md.topics[index]
            let replication = (0..<Int(topic.partition_cnt)).map { Int(topic.partitions[$0].replica_cnt) }.max() ?? 0
            return TopicSummary(name: String(cString: topic.topic), partitionCount: Int(topic.partition_cnt), replicationFactor: replication)
        }
    }

    /// Partitions of the one topic a metadata call was limited to, without offsets yet.
    static func partitions(of name: String, in metadata: RdMetadata) throws -> [PartitionInfo] {
        let md = metadata.pointer.pointee
        guard md.topic_cnt >= 1 else { throw KafkaError.unknownTopic(name) }
        let topic = md.topics[0]
        if topic.err != RD_KAFKA_RESP_ERR_NO_ERROR { throw RdFailure.code(topic.err.rawValue) }
        return (0..<Int(topic.partition_cnt)).map { index in
            let partition = topic.partitions[index]
            let replicas = (0..<Int(partition.replica_cnt)).map { Int(partition.replicas[$0]) }
            let isr = (0..<Int(partition.isr_cnt)).map { Int(partition.isrs[$0]) }
            return PartitionInfo(
                id: Int(partition.id), leader: partition.leader >= 0 ? Int(partition.leader) : nil,
                replicas: replicas, isr: isr, startOffset: 0, endOffset: 0)
        }.sorted { $0.id < $1.id }
    }

    /// A leaderless partition can't answer, so it keeps 0/0 rather than failing the whole topic.
    func watermarks(for partition: PartitionInfo, topic: String, session: Session) async throws -> PartitionInfo {
        guard partition.leader != nil else { return partition }
        let (low, high) = try Self.queryWatermarks(client: session.client, topic: topic, partition: partition.id, timeoutMs: timeoutMs)
        return PartitionInfo(
            id: partition.id, leader: partition.leader, replicas: partition.replicas, isr: partition.isr,
            startOffset: low, endOffset: high)
    }

    /// Blocking. A leader that moved or was just elected answers "not leader" for a moment; that is retried a few times.
    static func queryWatermarks(client: RdClient, topic: String, partition: Int, timeoutMs: Int32) throws -> (low: Int64, high: Int64) {
        var low: Int64 = 0
        var high: Int64 = 0
        var code = RD_KAFKA_RESP_ERR_NO_ERROR
        for attempt in 0..<watermarkAttempts {
            code = rd_kafka_query_watermark_offsets(client.pointer, topic, Int32(partition), &low, &high, timeoutMs)
            let transient = code == RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION || code == RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE
            if !transient || attempt == watermarkAttempts - 1 { break }
            Thread.sleep(forTimeInterval: 0.2)
        }
        guard code == RD_KAFKA_RESP_ERR_NO_ERROR else { throw RdFailure.code(code.rawValue) }
        return (low, high)
    }

    static var watermarkAttempts: Int { 5 }

    // MARK: Admin requests

    /// Sends one admin request and waits for its result event. `send` receives the options and the result queue.
    private func runAdmin(
        _ operation: rd_kafka_admin_op_t, expecting eventType: Int32, session: Session,
        send: (OpaquePointer, OpaquePointer) -> Void
    ) throws -> RdEvent {
        let options = try RdAdminOptions(client: session.client, operation: operation, timeoutMs: timeoutMs)
        let results = try RdQueue(client: session.client)
        send(options.pointer, results.pointer)
        // The request timeout is enforced by librdkafka; the extra second only covers its own scheduling.
        guard let event = results.poll(timeoutMs: timeoutMs + 1000) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__TIMED_OUT.rawValue) }
        if event.errorCode != RD_KAFKA_RESP_ERR_NO_ERROR.rawValue { throw RdFailure.code(event.errorCode) }
        guard event.type == eventType else { throw RdFailure.code(RD_KAFKA_RESP_ERR__BAD_MSG.rawValue) }
        return event
    }

    func createTopic(_ spec: NewTopic, session: Session) throws {
        let (pointer, message) = withErrorBuffer {
            rd_kafka_NewTopic_new(spec.name, Int32(clamping: spec.partitions), Int32(clamping: spec.replicationFactor), $0, $1)
        }
        guard let pointer else { throw RdFailure.config(message) }
        let topic = RdAdminObject(pointer) { rd_kafka_NewTopic_destroy($0) }
        for (key, value) in spec.config.sorted(by: { $0.key < $1.key }) {
            guard rd_kafka_NewTopic_set_config(topic.pointer, key, value) == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw RdFailure.code(RD_KAFKA_RESP_ERR__INVALID_ARG.rawValue)
            }
        }
        var topics: [OpaquePointer?] = [topic.pointer]
        let event = try runAdmin(RD_KAFKA_ADMIN_OP_CREATETOPICS, expecting: RD_KAFKA_EVENT_CREATETOPICS_RESULT, session: session) { options, queue in
            topics.withUnsafeMutableBufferPointer { rd_kafka_CreateTopics(session.client.pointer, $0.baseAddress, 1, options, queue) }
        }
        guard let result = rd_kafka_event_CreateTopics_result(event.pointer) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__BAD_MSG.rawValue) }
        var count = 0
        let results = rd_kafka_CreateTopics_result_topics(result, &count)
        for index in 0..<count {
            if let item = results?[index] {
                let code = rd_kafka_topic_result_error(item)
                if code != RD_KAFKA_RESP_ERR_NO_ERROR { throw RdFailure.code(code.rawValue) }
            }
        }
    }

    func deleteTopic(_ name: String, session: Session) throws {
        guard let pointer = rd_kafka_DeleteTopic_new(name) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__INVALID_ARG.rawValue) }
        let topic = RdAdminObject(pointer) { rd_kafka_DeleteTopic_destroy($0) }
        var topics: [OpaquePointer?] = [topic.pointer]
        let event = try runAdmin(RD_KAFKA_ADMIN_OP_DELETETOPICS, expecting: RD_KAFKA_EVENT_DELETETOPICS_RESULT, session: session) { options, queue in
            topics.withUnsafeMutableBufferPointer { rd_kafka_DeleteTopics(session.client.pointer, $0.baseAddress, 1, options, queue) }
        }
        guard let result = rd_kafka_event_DeleteTopics_result(event.pointer) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__BAD_MSG.rawValue) }
        var count = 0
        let results = rd_kafka_DeleteTopics_result_topics(result, &count)
        for index in 0..<count {
            if let item = results?[index] {
                let code = rd_kafka_topic_result_error(item)
                if code != RD_KAFKA_RESP_ERR_NO_ERROR { throw RdFailure.code(code.rawValue) }
            }
        }
    }

    /// The topic's settings with default/sensitive flags. `isDefault` means "not overridden on the topic".
    func describeConfig(of name: String, session: Session) throws -> [ConfigEntry] {
        guard let pointer = rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_TOPIC, name) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__INVALID_ARG.rawValue) }
        let resource = RdAdminObject(pointer) { rd_kafka_ConfigResource_destroy($0) }
        var resources: [OpaquePointer?] = [resource.pointer]
        let event = try runAdmin(RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS, expecting: RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT, session: session) { options, queue in
            resources.withUnsafeMutableBufferPointer { rd_kafka_DescribeConfigs(session.client.pointer, $0.baseAddress, 1, options, queue) }
        }
        guard let result = rd_kafka_event_DescribeConfigs_result(event.pointer) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__BAD_MSG.rawValue) }
        var resourceCount = 0
        guard let described = rd_kafka_DescribeConfigs_result_resources(result, &resourceCount), resourceCount >= 1, let first = described[0] else {
            throw RdFailure.code(RD_KAFKA_RESP_ERR__BAD_MSG.rawValue)
        }
        let code = rd_kafka_ConfigResource_error(first)
        if code != RD_KAFKA_RESP_ERR_NO_ERROR { throw RdFailure.code(code.rawValue) }
        var entryCount = 0
        guard let entries = rd_kafka_ConfigResource_configs(first, &entryCount) else { return [] }
        return (0..<entryCount).compactMap { index in
            guard let entry = entries[index], let namePointer = rd_kafka_ConfigEntry_name(entry) else { return nil }
            let value = rd_kafka_ConfigEntry_value(entry).map { String(cString: $0) }
            let overridden = rd_kafka_ConfigEntry_source(entry) == RD_KAFKA_CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG
            return ConfigEntry(
                name: String(cString: namePointer), value: value, isDefault: !overridden,
                isSensitive: rd_kafka_ConfigEntry_is_sensitive(entry) != 0)
        }
    }
}
