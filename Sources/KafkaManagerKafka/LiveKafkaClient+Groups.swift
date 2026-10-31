import CRdKafka
import Foundation
import KafkaManagerCore

struct TopicPartition: Hashable {
    let topic: String
    let partition: Int
}

// Consumer groups, read only: nothing here commits, joins or alters anything.
extension LiveKafkaClient {
    /// Protocol types that count as consumer groups. A group that only ever had commits (no members ever joined) has an
    /// empty protocol type and is still a consumer group in every tool; Connect, Streams-internal and others are not listed.
    static let consumerProtocolTypes: Set<String> = ["consumer", ""]

    /// One `rd_kafka_list_groups` call. It is preferred over `ListConsumerGroups` because it also returns the member count
    /// and the protocol type, which the list shows; `ListConsumerGroups` has the state but neither.
    public func listGroups() async throws -> [GroupSummary] {
        let session = try requireSession()
        var list: UnsafePointer<rd_kafka_group_list>?
        let code = rd_kafka_list_groups(session.client.pointer, nil, &list, timeoutMs)
        // PARTIAL: some brokers did not answer; what arrived is still a useful list.
        guard code == RD_KAFKA_RESP_ERR_NO_ERROR || code == RD_KAFKA_RESP_ERR__PARTIAL, let list else {
            throw map(RdFailure.code(code.rawValue), session: session, resource: session.host)
        }
        defer { rd_kafka_group_list_destroy(list) }
        var summaries: [GroupSummary] = []
        for index in 0..<Int(list.pointee.group_cnt) {
            let group = list.pointee.groups[index]
            guard group.err == RD_KAFKA_RESP_ERR_NO_ERROR else { continue }
            let protocolType = group.protocol_type.map { String(cString: $0) } ?? ""
            guard Self.consumerProtocolTypes.contains(protocolType) else { continue }
            summaries.append(GroupSummary(
                id: String(cString: group.group), state: group.state.map { String(cString: $0) } ?? "Unknown",
                memberCount: Int(group.member_cnt)))
        }
        return summaries.sorted { $0.id < $1.id }
    }

    /// DescribeConsumerGroups (state, coordinator, assignor, members with parsed assignments), ListConsumerGroupOffsets
    /// (all committed offsets) and two batched ListOffsets calls (start and end of every involved partition).
    public func describeGroup(_ id: String) async throws -> GroupDetail {
        let session = try requireSession()
        do {
            let description = try describeMembers(of: id, session: session)
            if description.isGone { throw KafkaError.clusterError(code: KafkaError.groupIdNotFoundCode, message: "group not found") }
            try Task.checkCancellation()
            let committed = try committedOffsets(of: id, session: session)
            var involved = Set(committed.keys)
            for member in description.members {
                for (topic, partitions) in member.assignments { involved.formUnion(partitions.map { TopicPartition(topic: topic, partition: $0) }) }
            }
            let ordered = involved.sorted { ($0.topic, $0.partition) < ($1.topic, $1.partition) }
            try Task.checkCancellation()
            let earliest = try await offsets(of: ordered, spec: RD_KAFKA_OFFSET_SPEC_EARLIEST, session: session, missingIsUnknownTopic: true)
            let latest = try await offsets(of: ordered, spec: RD_KAFKA_OFFSET_SPEC_LATEST, session: session, missingIsUnknownTopic: true)
            let lags = ordered.map { key in
                PartitionLag(
                    topic: key.topic, partition: key.partition, committedOffset: committed[key],
                    startOffset: earliest[key] ?? 0, endOffset: latest[key] ?? 0)
            }
            return GroupDetail(
                id: id, state: description.state, members: description.members, lags: lags,
                coordinatorId: description.coordinatorId, coordinatorHost: description.coordinatorHost, assignor: description.assignor)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw map(error, session: session, resource: id)
        }
    }

    // MARK: Pieces

    private struct Description {
        var state = "Unknown"
        var members: [GroupMemberInfo] = []
        var coordinatorId: Int?
        var coordinatorHost: String?
        var assignor: String?
        /// The broker answers "Dead" for a group it doesn't know.
        var isGone: Bool { state == "Dead" && members.isEmpty }
    }

    private func describeMembers(of id: String, session: Session) throws -> Description {
        let event = try id.withCString { name -> RdEvent in
            var names: [UnsafePointer<CChar>?] = [name]
            return try runAdmin(RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS, expecting: RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT, session: session) { options, queue in
                names.withUnsafeMutableBufferPointer { rd_kafka_DescribeConsumerGroups(session.client.pointer, $0.baseAddress, 1, options, queue) }
            }
        }
        guard let result = rd_kafka_event_DescribeConsumerGroups_result(event.pointer) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__BAD_MSG.rawValue) }
        var count = 0
        guard let groups = rd_kafka_DescribeConsumerGroups_result_groups(result, &count), count >= 1, let group = groups[0] else {
            throw RdFailure.code(RD_KAFKA_RESP_ERR__BAD_MSG.rawValue)
        }
        if let error = rd_kafka_ConsumerGroupDescription_error(group) {
            let code = rd_kafka_error_code(error)
            if code != RD_KAFKA_RESP_ERR_NO_ERROR { throw RdFailure.code(code.rawValue) }
        }
        var description = Description()
        description.state = String(cString: rd_kafka_consumer_group_state_name(rd_kafka_ConsumerGroupDescription_state(group)))
        description.assignor = rd_kafka_ConsumerGroupDescription_partition_assignor(group).map { String(cString: $0) }.flatMap { $0.isEmpty ? nil : $0 }
        if let node = rd_kafka_ConsumerGroupDescription_coordinator(group) {
            description.coordinatorId = Int(rd_kafka_Node_id(node))
            description.coordinatorHost = rd_kafka_Node_host(node).map { "\(String(cString: $0)):\(rd_kafka_Node_port(node))" }
        }
        for index in 0..<rd_kafka_ConsumerGroupDescription_member_count(group) {
            guard let member = rd_kafka_ConsumerGroupDescription_member(group, index) else { continue }
            var assignments: [String: [Int]] = [:]
            if let list = rd_kafka_MemberAssignment_partitions(rd_kafka_MemberDescription_assignment(member)) {
                for element in 0..<Int(list.pointee.cnt) {
                    let item = list.pointee.elems[element]
                    assignments[String(cString: item.topic), default: []].append(Int(item.partition))
                }
            }
            description.members.append(GroupMemberInfo(
                id: rd_kafka_MemberDescription_consumer_id(member).map { String(cString: $0) } ?? "",
                clientId: rd_kafka_MemberDescription_client_id(member).map { String(cString: $0) } ?? "",
                host: rd_kafka_MemberDescription_host(member).map { String(cString: $0) } ?? "",
                assignments: assignments.mapValues { $0.sorted() }))
        }
        return description
    }

    /// Every committed offset of the group; a partition without a commit is not in the result.
    private func committedOffsets(of id: String, session: Session) throws -> [TopicPartition: Int64] {
        let request = RdAdminObject(try Self.listOffsetsRequest(group: id)) { rd_kafka_ListConsumerGroupOffsets_destroy($0) }
        var requests: [OpaquePointer?] = [request.pointer]
        let event = try runAdmin(RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS, expecting: RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT, session: session) { options, queue in
            requests.withUnsafeMutableBufferPointer { rd_kafka_ListConsumerGroupOffsets(session.client.pointer, $0.baseAddress, 1, options, queue) }
        }
        guard let result = rd_kafka_event_ListConsumerGroupOffsets_result(event.pointer) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__BAD_MSG.rawValue) }
        var count = 0
        guard let groups = rd_kafka_ListConsumerGroupOffsets_result_groups(result, &count), count >= 1, let group = groups[0] else { return [:] }
        if let error = rd_kafka_group_result_error(group) {
            let code = rd_kafka_error_code(error)
            if code != RD_KAFKA_RESP_ERR_NO_ERROR { throw RdFailure.code(code.rawValue) }
        }
        guard let list = rd_kafka_group_result_partitions(group) else { return [:] }
        var committed: [TopicPartition: Int64] = [:]
        for index in 0..<Int(list.pointee.cnt) {
            let item = list.pointee.elems[index]
            // -1 (invalid offset) means "no commit" for that partition.
            guard item.err == RD_KAFKA_RESP_ERR_NO_ERROR, item.offset >= 0 else { continue }
            committed[TopicPartition(topic: String(cString: item.topic), partition: Int(item.partition))] = item.offset
        }
        return committed
    }

    private static func listOffsetsRequest(group: String) throws -> OpaquePointer {
        guard let request = rd_kafka_ListConsumerGroupOffsets_new(group, nil) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__INVALID_ARG.rawValue) }
        return request
    }

    /// Start or end offsets of many partitions with one ListOffsets request (all topics together). A leader that was just
    /// elected answers "not leader" for a moment; only those partitions are asked again, a few times.
    ///
    /// - Parameter missingIsUnknownTopic: partitions of a deleted topic come back as 0 instead of failing the call, so a
    ///   group that still has commits for it shows them as "committed ahead of end".
    func offsets(
        of partitions: [TopicPartition], spec: rd_kafka_OffsetSpec_t, session: Session, missingIsUnknownTopic: Bool = false
    ) async throws -> [TopicPartition: Int64] {
        var result: [TopicPartition: Int64] = [:]
        var pending = partitions
        for attempt in 0..<Self.watermarkAttempts where !pending.isEmpty {
            let answers = try requestOffsets(pending, spec: spec, session: session)
            var retry: [TopicPartition] = []
            for (key, answer) in answers {
                switch answer.error {
                case RD_KAFKA_RESP_ERR_NO_ERROR.rawValue: result[key] = answer.offset
                case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION.rawValue, RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE.rawValue:
                    if attempt == Self.watermarkAttempts - 1 { throw RdFailure.code(answer.error) }
                    retry.append(key)
                case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART.rawValue where missingIsUnknownTopic: result[key] = 0
                default: throw RdFailure.code(answer.error)
                }
            }
            pending = retry
            if !pending.isEmpty { try await Task.sleep(for: .milliseconds(200)) }
        }
        return result
    }

    private func requestOffsets(
        _ partitions: [TopicPartition], spec: rd_kafka_OffsetSpec_t, session: Session
    ) throws -> [TopicPartition: (offset: Int64, error: Int32)] {
        guard !partitions.isEmpty else { return [:] }
        let list = try RdPartitionList(capacity: partitions.count)
        for key in partitions { list.add(topic: key.topic, partition: key.partition, offset: Int64(spec.rawValue)) }
        let event = try runAdmin(RD_KAFKA_ADMIN_OP_LISTOFFSETS, expecting: RD_KAFKA_EVENT_LISTOFFSETS_RESULT, session: session) { options, queue in
            rd_kafka_ListOffsets(session.client.pointer, list.pointer, options, queue)
        }
        guard let result = rd_kafka_event_ListOffsets_result(event.pointer) else { throw RdFailure.code(RD_KAFKA_RESP_ERR__BAD_MSG.rawValue) }
        var count = 0
        guard let infos = rd_kafka_ListOffsets_result_infos(result, &count) else { return [:] }
        var answers: [TopicPartition: (offset: Int64, error: Int32)] = [:]
        for index in 0..<count {
            guard let info = infos[index], let item = rd_kafka_ListOffsetsResultInfo_topic_partition(info) else { continue }
            answers[TopicPartition(topic: String(cString: item.pointee.topic), partition: Int(item.pointee.partition))] =
                (item.pointee.offset, item.pointee.err.rawValue)
        }
        return answers
    }
}
