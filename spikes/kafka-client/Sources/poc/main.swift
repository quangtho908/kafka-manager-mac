import CRdkafka
import Foundation

// MARK: Measurement helpers

func memory() -> (rssMB: Double, footprintMB: Double, threads: Int) {
    var basic = mach_task_basic_info()
    var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size / MemoryLayout<natural_t>.size)
    _ = withUnsafeMutablePointer(to: &basic) {
        $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) { task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), $0, &count) }
    }
    var vm = task_vm_info_data_t()
    var vmCount = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<natural_t>.size)
    _ = withUnsafeMutablePointer(to: &vm) {
        $0.withMemoryRebound(to: integer_t.self, capacity: Int(vmCount)) { task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &vmCount) }
    }
    var list: thread_act_array_t?
    var threadCount: mach_msg_type_number_t = 0
    task_threads(mach_task_self_, &list, &threadCount)
    return (Double(basic.resident_size) / 1_048_576, Double(vm.phys_footprint) / 1_048_576, Int(threadCount))
}

func report(_ label: String) {
    let m = memory()
    print(String(format: "RSS %7.1f MB | footprint %7.1f MB | threads %2d | %@", m.rssMB, m.footprintMB, m.threads, label))
}

func timed<T>(_ label: String, _ body: () throws -> T) rethrows -> T {
    let start = ContinuousClock.now
    let result = try body()
    let ms = Double((ContinuousClock.now - start).components.attoseconds / 1_000_000_000_000_000) + Double((ContinuousClock.now - start).components.seconds) * 1000
    print(String(format: "  [%7.1f ms] %@", ms, label))
    return result
}

struct SpikeError: Error, CustomStringConvertible { let description: String }

func check(_ err: rd_kafka_resp_err_t, _ what: String) throws {
    if err != RD_KAFKA_RESP_ERR_NO_ERROR { throw SpikeError(description: "\(what): \(String(cString: rd_kafka_err2str(err)))") }
}

// MARK: Thin wrapper over the C API (this is the kind of code LiveKafkaClient would contain)

func makeConf(_ props: [String: String]) throws -> OpaquePointer {
    guard let conf = rd_kafka_conf_new() else { throw SpikeError(description: "conf_new") }
    var err = [CChar](repeating: 0, count: 512)
    for (key, value) in props where rd_kafka_conf_set(conf, key, value, &err, err.count) != RD_KAFKA_CONF_OK {
        throw SpikeError(description: "conf \(key)=\(value): \(String(cString: err))")
    }
    return conf
}

func makeClient(_ type: rd_kafka_type_t, _ props: [String: String]) throws -> OpaquePointer {
    var err = [CChar](repeating: 0, count: 512)
    guard let rk = rd_kafka_new(type, try makeConf(props), &err, err.count) else {
        throw SpikeError(description: "rd_kafka_new: \(String(cString: err))")
    }
    return rk
}

/// Runs one admin request on a private queue and returns the result event (caller destroys it).
func admin(_ rk: OpaquePointer, _ op: rd_kafka_admin_op_t, timeoutMs: Int32 = 10_000, _ start: (OpaquePointer, OpaquePointer) -> Void) throws -> OpaquePointer {
    let queue = rd_kafka_queue_new(rk)
    defer { rd_kafka_queue_destroy(queue) }
    let options = rd_kafka_AdminOptions_new(rk, op)
    defer { rd_kafka_AdminOptions_destroy(options) }
    var err = [CChar](repeating: 0, count: 256)
    rd_kafka_AdminOptions_set_request_timeout(options, timeoutMs, &err, err.count)
    start(options!, queue!)
    guard let event = rd_kafka_queue_poll(queue, timeoutMs + 2_000) else { throw SpikeError(description: "admin timeout") }
    try check(rd_kafka_event_error(event), "admin: \(rd_kafka_event_error_string(event).map { String(cString: $0) } ?? "")")
    return event
}

func createTopic(_ rk: OpaquePointer, _ name: String, partitions: Int32) throws {
    var err = [CChar](repeating: 0, count: 256)
    var topics: [OpaquePointer?] = [rd_kafka_NewTopic_new(name, partitions, 1, &err, err.count)]
    defer { rd_kafka_NewTopic_destroy_array(&topics, 1) }
    let event = try admin(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS) { options, queue in
        rd_kafka_CreateTopics(rk, &topics, 1, options, queue)
    }
    defer { rd_kafka_event_destroy(event) }
    var count = 0
    let results = rd_kafka_CreateTopics_result_topics(rd_kafka_event_CreateTopics_result(event), &count)
    if let results, count > 0 {
        let code = rd_kafka_topic_result_error(results[0])
        if code != RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS { try check(code, "createTopic") }
    }
}

func deleteTopic(_ rk: OpaquePointer, _ name: String) throws {
    var topics: [OpaquePointer?] = [rd_kafka_DeleteTopic_new(name)]
    defer { rd_kafka_DeleteTopic_destroy_array(&topics, 1) }
    let event = try admin(rk, RD_KAFKA_ADMIN_OP_DELETETOPICS) { options, queue in
        rd_kafka_DeleteTopics(rk, &topics, 1, options, queue)
    }
    rd_kafka_event_destroy(event)
}

func deleteGroup(_ rk: OpaquePointer, _ name: String) throws {
    var groups: [OpaquePointer?] = [rd_kafka_DeleteGroup_new(name)]
    defer { rd_kafka_DeleteGroup_destroy_array(&groups, 1) }
    let event = try admin(rk, RD_KAFKA_ADMIN_OP_DELETEGROUPS) { options, queue in
        rd_kafka_DeleteGroups(rk, &groups, 1, options, queue)
    }
    rd_kafka_event_destroy(event)
}

func listTopics(_ rk: OpaquePointer) throws -> [String] {
    var metadata: UnsafePointer<rd_kafka_metadata>?
    try check(rd_kafka_metadata(rk, 1, nil, &metadata, 10_000), "metadata")
    defer { rd_kafka_metadata_destroy(metadata) }
    guard let md = metadata?.pointee else { return [] }
    return (0..<Int(md.topic_cnt)).map { String(cString: md.topics[$0].topic) }.sorted()
}

struct PartitionDetail { let id: Int32; let leader: Int32; let replicas: Int; let isr: Int; let low: Int64; let high: Int64 }

func describeTopic(_ rk: OpaquePointer, _ name: String) throws -> [PartitionDetail] {
    guard let rkt = rd_kafka_topic_new(rk, name, nil) else { throw SpikeError(description: "topic_new") }
    defer { rd_kafka_topic_destroy(rkt) }
    var metadata: UnsafePointer<rd_kafka_metadata>?
    try check(rd_kafka_metadata(rk, 0, rkt, &metadata, 10_000), "metadata(topic)")
    defer { rd_kafka_metadata_destroy(metadata) }
    guard let md = metadata?.pointee, md.topic_cnt == 1 else { throw SpikeError(description: "unknown topic \(name)") }
    let topic = md.topics[0]
    try check(topic.err, "topic \(name)")
    return try (0..<Int(topic.partition_cnt)).map { index in
        let partition = topic.partitions[index]
        var low: Int64 = 0, high: Int64 = 0
        try check(rd_kafka_query_watermark_offsets(rk, name, partition.id, &low, &high, 10_000), "watermarks")
        return PartitionDetail(id: partition.id, leader: partition.leader, replicas: Int(partition.replica_cnt), isr: Int(partition.isr_cnt), low: low, high: high)
    }
}

struct GroupInfo { let id: String; let state: String; let members: Int }

func listGroups(_ rk: OpaquePointer) throws -> [GroupInfo] {
    var list: UnsafePointer<rd_kafka_group_list>?
    try check(rd_kafka_list_groups(rk, nil, &list, 10_000), "list_groups")
    defer { rd_kafka_group_list_destroy(list) }
    guard let l = list?.pointee else { return [] }
    return (0..<Int(l.group_cnt)).map {
        let g = l.groups[$0]
        return GroupInfo(id: String(cString: g.group), state: String(cString: g.state), members: Int(g.member_cnt))
    }.sorted { $0.id < $1.id }
}

/// Committed offsets per "topic/partition" for a group, via the admin API (does not join the group).
func committedOffsets(_ rk: OpaquePointer, group: String) throws -> [String: Int64] {
    var requests: [OpaquePointer?] = [rd_kafka_ListConsumerGroupOffsets_new(group, nil)]
    defer { rd_kafka_ListConsumerGroupOffsets_destroy_array(&requests, 1) }
    let event = try admin(rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS) { options, queue in
        rd_kafka_ListConsumerGroupOffsets(rk, &requests, 1, options, queue)
    }
    defer { rd_kafka_event_destroy(event) }
    var count = 0
    var result: [String: Int64] = [:]
    guard let groups = rd_kafka_ListConsumerGroupOffsets_result_groups(rd_kafka_event_ListConsumerGroupOffsets_result(event), &count), count > 0,
          let partitions = rd_kafka_group_result_partitions(groups[0]) else { return result }
    for index in 0..<Int(partitions.pointee.cnt) {
        let element = partitions.pointee.elems[index]
        if element.err == RD_KAFKA_RESP_ERR_NO_ERROR, element.offset >= 0 {
            result["\(String(cString: element.topic))/\(element.partition)"] = element.offset
        }
    }
    return result
}


/// Fetching metadata for the topic first makes the partition leader known, so assign() starts fetching at once
/// instead of waiting for librdkafka's fixed 500 ms offset-query retry timer.
func warmUp(_ rk: OpaquePointer, topic: String) throws {
    guard let rkt = rd_kafka_topic_new(rk, topic, nil) else { throw SpikeError(description: "topic_new") }
    defer { rd_kafka_topic_destroy(rkt) }
    var metadata: UnsafePointer<rd_kafka_metadata>?
    try check(rd_kafka_metadata(rk, 0, rkt, &metadata, 10_000), "warm-up metadata")
    rd_kafka_metadata_destroy(metadata)
}

/// Reads up to `limit` messages from the given partitions starting at `offset`, with a consumer that has NO group.id.
func readMessages(bootstrap: String, tuning: [String: String], topic: String, partitions: [Int32], offset: Int64, limit: Int, groupId: String? = nil, commit: Bool = false, label: String? = nil) throws -> (count: Int, bytes: Int) {
    var props = ["bootstrap.servers": bootstrap, "enable.auto.commit": "false"].merging(tuning) { $1 }
    // librdkafka refuses assign() without a group.id ("Local: Unknown group"). A placeholder is safe: with assign() and no
    // commit the consumer never sends JoinGroup/OffsetCommit, so no group appears on the broker (verified in the report).
    props["group.id"] = groupId ?? "km-noop-never-committed"
    let rk = try makeClient(RD_KAFKA_CONSUMER, props)
    defer { rd_kafka_destroy(rk) }
    if !arguments.contains("--no-warmup") { try warmUp(rk, topic: topic) }
    let assignment = rd_kafka_topic_partition_list_new(Int32(partitions.count))
    defer { rd_kafka_topic_partition_list_destroy(assignment) }
    for partition in partitions { rd_kafka_topic_partition_list_add(assignment, topic, partition).pointee.offset = offset }
    try check(rd_kafka_assign(rk, assignment), "assign")
    var count = 0, bytes = 0
    var idle = 0
    let clock = ContinuousClock()
    let began = clock.now
    var firstMessage: Duration?
    while count < limit, idle < 200 {
        guard let message = rd_kafka_consumer_poll(rk, 50) else { idle += 1; continue }
        defer { rd_kafka_message_destroy(message) }
        if message.pointee.err == RD_KAFKA_RESP_ERR_NO_ERROR {
            count += 1
            bytes += message.pointee.len
            if firstMessage == nil { firstMessage = clock.now - began }
        } else if message.pointee.err != RD_KAFKA_RESP_ERR__PARTITION_EOF { idle += 1 }
    }
    let polled = clock.now - began
    // Simulates a viewer that keeps the consumer open (e.g. while the user reads the table): prefetch keeps filling.
    if let linger = arguments.firstIndex(of: "--linger").flatMap({ Int(arguments[$0 + 1]) }), label != nil { usleep(UInt32(linger * 1000)) }
    if let label { report("\(label): consumer still open, \(count) messages read") }
    if commit { try check(rd_kafka_commit(rk, nil, 0), "commit") }
    let closeStart = clock.now
    try check(rd_kafka_consumer_close(rk), "close")
    print("  read timing: first message after \(firstMessage.map { "\($0)" } ?? "never"), \(count) messages after \(polled), consumer_close took \(clock.now - closeStart)")
    return (count, bytes)
}


/// Same read using librdkafka's legacy "simple consumer" API: needs no group.id at all.
func readMessagesLegacy(bootstrap: String, tuning: [String: String], topic: String, partitions: [Int32], offset: Int64, limit: Int, label: String? = nil) throws -> (count: Int, bytes: Int) {
    let props = ["bootstrap.servers": bootstrap].merging(tuning) { $1 }
    let rk = try makeClient(RD_KAFKA_CONSUMER, props)
    defer { rd_kafka_destroy(rk) }
    guard let rkt = rd_kafka_topic_new(rk, topic, nil), let queue = rd_kafka_queue_new(rk) else { throw SpikeError(description: "topic/queue") }
    defer { rd_kafka_queue_destroy(queue); rd_kafka_topic_destroy(rkt) }
    for partition in partitions where rd_kafka_consume_start_queue(rkt, partition, offset, queue) != 0 { throw SpikeError(description: "consume_start") }
    var count = 0, bytes = 0, idle = 0
    let clock = ContinuousClock()
    let began = clock.now
    var firstMessage: Duration?
    while count < limit, idle < 200 {
        guard let message = rd_kafka_consume_queue(queue, 50) else { idle += 1; continue }
        defer { rd_kafka_message_destroy(message) }
        if message.pointee.err == RD_KAFKA_RESP_ERR_NO_ERROR {
            count += 1
            bytes += message.pointee.len
            if firstMessage == nil { firstMessage = clock.now - began }
        }
    }
    if let label { report("\(label): legacy consumer still open, \(count) messages read") }
    for partition in partitions { rd_kafka_consume_stop(rkt, partition) }
    print("  legacy read timing: first message after \(firstMessage.map { "\($0)" } ?? "never"), \(count) messages after \(clock.now - began)")
    return (count, bytes)
}

func produce(bootstrap: String, topic: String, count: Int, size: Int) throws {
    let rk = try makeClient(RD_KAFKA_PRODUCER, ["bootstrap.servers": bootstrap])
    defer { rd_kafka_destroy(rk) }
    guard let rkt = rd_kafka_topic_new(rk, topic, nil) else { throw SpikeError(description: "topic_new") }
    defer { rd_kafka_topic_destroy(rkt) }
    var payload = [UInt8](repeating: UInt8(ascii: "x"), count: size)
    for index in 0..<count {
        let key = Array("key-\(index)".utf8)
        // rd_kafka_producev is variadic and not importable into Swift; the non-variadic rd_kafka_produce is used instead.
        let result = rd_kafka_produce(rkt, Int32(index % 3), RD_KAFKA_MSG_F_COPY, &payload, size, key, key.count, nil)
        if result != 0 { throw SpikeError(description: "produce failed") }
        if index % 100 == 0 { rd_kafka_poll(rk, 0) }
    }
    try check(rd_kafka_flush(rk, 10_000), "flush")
}

// MARK: Scenario

let arguments = CommandLine.arguments
let bootstrap = arguments.firstIndex(of: "--bootstrap").map { arguments[$0 + 1] } ?? "localhost:9092"
let legacy = arguments.contains("--legacy")
let tuned = arguments.contains("--tuned")
let seedCount = arguments.firstIndex(of: "--bulk").flatMap { Int(arguments[$0 + 1]) } ?? 600
let topic = "spike-orders"
let groupName = "spike-g"
// Buffer settings under test. Defaults (librdkafka 2.16): queued.max.messages.kbytes=65536, queued.min.messages=100000,
// fetch.max.bytes=52428800, fetch.message.max.bytes=1048576, fetch.wait.max.ms=500.
let extraProps: [String: String] = Dictionary(uniqueKeysWithValues: arguments.indices.filter { arguments[$0] == "--prop" }.compactMap { index -> (String, String)? in
    let parts = arguments[index + 1].split(separator: "=", maxSplits: 1).map(String.init)
    return parts.count == 2 ? (parts[0], parts[1]) : nil
})
let tuning: [String: String] = extraProps.merging(tuned ? [
    "queued.max.messages.kbytes": "1024", "queued.min.messages": "100",
    "fetch.max.bytes": "1048576", "fetch.message.max.bytes": "262144", "fetch.wait.max.ms": "50",
] : [:]) { $1 }

print("== librdkafka \(String(cString: rd_kafka_version_str())) | \(tuned ? "TUNED" : "DEFAULT") buffers | broker \(bootstrap)")
report("process start (before any Kafka call)")

if let seedIndex = arguments.firstIndex(of: "--seed"), let n = Int(arguments[seedIndex + 1]) {
    do {
        let rk = try makeClient(RD_KAFKA_PRODUCER, ["bootstrap.servers": bootstrap])
        try createTopic(rk, topic, partitions: 3)
        rd_kafka_destroy(rk)
        try produce(bootstrap: bootstrap, topic: topic, count: n, size: 2048)
        print("seeded \(n) x 2KB into \(topic)")
    } catch { print("FAILED: \(error)"); exit(1) }
    exit(0)
}

if arguments.contains("--ssl-smoke") {
    // Proves the static OpenSSL build is linked and accepted; a live handshake needs a TLS/SASL broker (not available here).
    for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
        let props = ["bootstrap.servers": "localhost:1", "security.protocol": "sasl_ssl", "sasl.mechanism": mechanism,
                     "sasl.username": "u", "sasl.password": "p", "enable.ssl.certificate.verification": "false"]
        do { let rk = try makeClient(RD_KAFKA_PRODUCER, props); rd_kafka_destroy(rk); print("sasl_ssl + \(mechanism): client created OK") }
        catch { print("sasl_ssl + \(mechanism): \(error)") }
    }
    exit(0)
}

if arguments.contains("--snapshot") || arguments.contains("--cleanup") {
    do {
        let rk = try makeClient(RD_KAFKA_PRODUCER, ["bootstrap.servers": bootstrap])
        defer { rd_kafka_destroy(rk) }
        if arguments.contains("--cleanup") {
            try? deleteGroup(rk, groupName)
            try? deleteTopic(rk, topic)
        }
        let groups = try listGroups(rk)
        print("groups: \(groups.map { "\($0.id)[\($0.state)]" })")
        for group in groups { print("  \(group.id): \(try committedOffsets(rk, group: group.id).sorted { $0.key < $1.key })") }
        print("topics: \(try listTopics(rk))")
    } catch { print("FAILED: \(error)"); exit(1) }
    exit(0)
}

do {
    let admin = try timed("create admin client (producer-type handle, no explicit connect)") { try makeClient(RD_KAFKA_PRODUCER, ["bootstrap.servers": bootstrap]) }
    report("after creating client (no request yet)")

    let topics = try timed("listTopics (all)") { try listTopics(admin) }
    print("  topics on broker: \(topics.count)")
    report("after connect + listTopics")

    if !arguments.contains("--skip-seed") {
        try createTopic(admin, topic, partitions: 3)
        try timed("produce \(seedCount) x 2KB") { try produce(bootstrap: bootstrap, topic: topic, count: seedCount, size: 2048) }
        report("after producing seed data")
    }

    let details = try timed("describeTopic \(topic)") { try describeTopic(admin, topic) }
    for p in details { print("  partition \(p.id): leader \(p.leader) replicas \(p.replicas) isr \(p.isr) low \(p.low) high \(p.high) count \(p.high - p.low)") }

    let groupsBefore = try listGroups(admin)
    print("  groups before reads: \(groupsBefore.map(\.id))")

    let first10 = try timed("read 10 messages (partition 0, earliest)") { try (legacy ? readMessagesLegacy(bootstrap: bootstrap, tuning: tuning, topic: topic, partitions: [0], offset: Int64(RD_KAFKA_OFFSET_BEGINNING), limit: 10) : readMessages(bootstrap: bootstrap, tuning: tuning, topic: topic, partitions: [0], offset: Int64(RD_KAFKA_OFFSET_BEGINNING), limit: 10)) }
    print("  got \(first10.count) messages, \(first10.bytes) bytes")
    report("after 10-message consumer closed")

    let read500 = try timed("read 500 messages (3 partitions, earliest; create+assign+poll+close)") { try (legacy ? readMessagesLegacy(bootstrap: bootstrap, tuning: tuning, topic: topic, partitions: [0, 1, 2], offset: Int64(RD_KAFKA_OFFSET_BEGINNING), limit: 500, label: "500-read") : readMessages(bootstrap: bootstrap, tuning: tuning, topic: topic, partitions: [0, 1, 2], offset: Int64(RD_KAFKA_OFFSET_BEGINNING), limit: 500, label: "500-read")) }
    print("  got \(read500.count) messages, \(read500.bytes) bytes")
    report("after 500-message consumer closed+destroyed")

    let groupsAfter = try listGroups(admin)
    print("  groups after reads: \(groupsAfter.map(\.id)) -> unchanged: \(groupsAfter.map(\.id) == groupsBefore.map(\.id))")

    // Lag demo: a group that committed 5 messages on partition 0.
    _ = try readMessages(bootstrap: bootstrap, tuning: tuning, topic: topic, partitions: [0], offset: Int64(RD_KAFKA_OFFSET_BEGINNING), limit: 5, groupId: groupName, commit: true)
    print("  groups with demo group: \(try listGroups(admin).map { "\($0.id)[\($0.state)]" })")
    let committed = try committedOffsets(admin, group: groupName)
    for p in details {
        let c = committed["\(topic)/\(p.id)"]
        print("  lag \(groupName) \(topic)/\(p.id): committed \(c.map(String.init) ?? "none") end \(p.high) lag \(c.map { String(p.high - $0) } ?? "no offset -> \(p.high - p.low)")")
    }

    if !arguments.contains("--keep") {
        try deleteGroup(admin, groupName)
        try deleteTopic(admin, topic)
        print("  cleaned up \(groupName) and \(topic)")
    }
    timed("destroy admin client") { rd_kafka_destroy(admin) }
    report("after destroying all clients")
    var usage = rusage()
    getrusage(RUSAGE_SELF, &usage)
    print(String(format: "peak RSS (ru_maxrss) %.1f MB", Double(usage.ru_maxrss) / 1_048_576))
} catch {
    print("FAILED: \(error)")
    exit(1)
}
