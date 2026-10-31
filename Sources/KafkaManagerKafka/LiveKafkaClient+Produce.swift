import CRdKafka
import Foundation
import KafkaManagerCore

extension LiveKafkaClient {
    /// Extra time beyond the client timeout for librdkafka to report its own `message.timeout.ms` expiry, so the error
    /// carries the real cause (all brokers down, bad credentials) instead of the wait running out first.
    private static let reportGraceMs: Int32 = 1500
    private static let pollSliceMs: Int32 = 50
    /// Room for the batch overhead on top of the largest message the app allows.
    private static let messageHeadroom = 65_536

    public func produce(_ request: ProduceRequest) async throws -> ProduceResult {
        let session = try requireSession()
        if let error = ProduceValidator.error(for: ProduceValidator.issues(for: request)) { throw error }
        try Task.checkCancellation()
        do {
            try checkTarget(request, session: session)
            let handle = try openProducer(session)
            producerUse += 1
            defer { scheduleProducerClose() }
            return try Self.deliver(request, on: handle, timeoutMs: timeoutMs, host: session.host, secrets: session.secrets)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw map(error, session: session, resource: request.topic)
        }
    }

    // MARK: Target

    /// The topic must exist (the producer never creates one) and an explicit partition must be one of its partitions.
    private func checkTarget(_ request: ProduceRequest, session: Session) throws {
        let topic = try RdTopic(client: session.client, name: request.topic)
        let metadata = try RdMetadata(client: session.client, allTopics: false, topic: topic, timeoutMs: timeoutMs)
        let partitions = try Self.partitions(of: request.topic, in: metadata)
        if let requested = request.partition, !partitions.contains(where: { $0.id == requested }) {
            throw KafkaError.clusterError(
                code: KafkaError.invalidProduceRequestCode,
                message: String(localized: "Topic \"\(request.topic)\" has no partition \(requested)."))
        }
    }

    // MARK: Producer handle

    private func openProducer(_ session: Session) throws -> ProducerHandle {
        if let producer { return producer }
        guard let settings = consumerSettings.withLock({ $0 }) else { throw KafkaError.unreachable(host: session.host) }
        let handle = try Self.makeProducer(settings: settings, timeoutMs: timeoutMs)
        producer = handle
        return handle
    }

    /// A producer tuned small: one message in flight at a time, no batching delay, no topic creation.
    static func makeProducer(settings: ConsumerSettings, timeoutMs: Int32) throws -> ProducerHandle {
        let sink = ErrorSink()
        let tuning: [(key: String, value: String)] = [
            ("acks", "all"), ("enable.idempotence", "true"), ("linger.ms", "0"),
            ("queue.buffering.max.messages", "100"), ("queue.buffering.max.kbytes", "4096"),
            ("message.max.bytes", String(ProduceValidator.defaultMaxMessageBytes + messageHeadroom)),
            ("message.timeout.ms", String(timeoutMs)), ("allow.auto.create.topics", "false"),
        ]
        let client = try makeClient(
            properties: settings.properties + tuning, secrets: settings.secrets, sink: sink, type: RD_KAFKA_PRODUCER,
            configure: { $0.setDeliveryCallback(DeliveryBox.callback) })
        return ProducerHandle(client: client, sink: sink)
    }

    func closeProducer() {
        producerIdleTask?.cancel()
        producerIdleTask = nil
        producer?.drain()
        producer = nil
    }

    private func scheduleProducerClose() {
        producerIdleTask?.cancel()
        let use = producerUse
        let idle = producerIdle
        producerIdleTask = Task { [weak self] in
            try? await Task.sleep(for: idle)
            guard !Task.isCancelled else { return }
            await self?.closeProducerIfUnused(since: use)
        }
    }

    private func closeProducerIfUnused(since use: Int) {
        if producerUse == use { closeProducer() }
    }

    // MARK: Delivery

    /// Queues the message and polls until its delivery report arrives. Blocking, on the client's executor; bounded by
    /// the client timeout. The box is retained for librdkafka and released by the callback, so every path below makes
    /// sure the callback has fired (by waiting, or by purging and polling) before it returns.
    static func deliver(
        _ request: ProduceRequest, on handle: ProducerHandle, timeoutMs: Int32, host: String, secrets: [String]
    ) throws -> ProduceResult {
        let box = DeliveryBox()
        let opaque = Unmanaged.passRetained(box)
        do {
            try RdProduce.enqueue(
                client: handle.client, topic: request.topic, partition: Int32(request.partition ?? -1),
                key: request.key, value: request.value,
                headers: request.headers.map { RdProduce.Header(name: $0.key, value: $0.value) },
                timestampMs: request.timestamp.map { Int64(($0.timeIntervalSince1970 * 1000).rounded()) },
                opaque: opaque.toOpaque())
        } catch {
            // Never queued, so no callback will come.
            opaque.release()
            throw error
        }
        let deadline = ContinuousClock.now + .milliseconds(Int(timeoutMs) + Int(reportGraceMs))
        while box.outcome == nil {
            _ = rd_kafka_poll(handle.client.pointer, pollSliceMs)
            if box.outcome != nil { break }
            if Task.isCancelled || ContinuousClock.now >= deadline {
                handle.drain()
                if Task.isCancelled { throw CancellationError() }
                throw KafkaError.timeout
            }
        }
        switch box.outcome {
        case .delivered(let partition, let offset, let timestampMs)?:
            return ProduceResult(partition: partition, offset: offset, timestamp: Date(timeIntervalSince1970: Double(timestampMs) / 1000))
        case .failed(let code)?:
            // A message that timed out in the queue is explained by what the client's own threads reported meanwhile.
            let effective = code == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT.rawValue ? RD_KAFKA_RESP_ERR__TIMED_OUT.rawValue : code
            throw Self.map(
                RdFailure.code(effective), sink: handle.sink, host: host, secrets: secrets, resource: request.topic)
        case nil:
            throw KafkaError.timeout
        }
    }
}
