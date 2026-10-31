import Testing
import Foundation
@testable import KafkaManagerCore

private func request(
    topic: String = "orders", partition: Int? = nil, key: Data? = nil, value: Data? = Data("v".utf8),
    headers: [MessageHeader] = [], timestamp: Date? = nil
) -> ProduceRequest {
    ProduceRequest(topic: topic, partition: partition, key: key, value: value, headers: headers, timestamp: timestamp)
}

@Suite("ProduceValidator")
struct ProduceValidatorTests {
    @Test func aPlainRequestIsValid() {
        #expect(ProduceValidator.issues(for: request()).isEmpty)
        #expect(ProduceValidator.issues(for: request(partition: 0, key: Data("k".utf8), value: nil)).isEmpty)
    }

    @Test(arguments: [
        (request(topic: ""), ProduceIssue.emptyTopic),
        (request(partition: -1), .negativePartition),
        (request(headers: [MessageHeader(key: "", value: nil)]), .emptyHeaderKey(index: 0)),
        (request(headers: [MessageHeader(key: "ok", value: nil), MessageHeader(key: "", value: nil)]), .emptyHeaderKey(index: 1)),
        (request(headers: [MessageHeader(key: "a\0b", value: nil)]), .headerKeyHasNul(index: 0)),
        (request(timestamp: Date(timeIntervalSince1970: -1)), .timestampBeforeEpoch),
    ])
    func eachProblemIsReported(item: ProduceRequest, expected: ProduceIssue) {
        #expect(ProduceValidator.issues(for: item) == [expected])
        #expect(!expected.message.isEmpty)
    }

    @Test func sizeTextIsNumericEvenForNothing() {
        #expect(ProduceValidator.sizeText(0).hasPrefix("0"))
        #expect(!ProduceValidator.sizeText(0).contains("Zero"))
    }

    @Test func sizeCountsKeyValueAndHeaders() {
        let item = request(key: Data(count: 10), value: Data(count: 100), headers: [MessageHeader(key: "abc", value: Data(count: 7)), MessageHeader(key: "é", value: nil)])
        #expect(ProduceValidator.size(of: item) == 10 + 100 + 3 + 7 + 2)
        #expect(ProduceValidator.size(of: request(value: nil)) == 0)
    }

    @Test func theSizeLimitIsInclusive() {
        let limit = 1000
        #expect(ProduceValidator.issues(for: request(value: Data(count: limit)), maxBytes: limit).isEmpty)
        #expect(ProduceValidator.issues(for: request(value: Data(count: limit + 1)), maxBytes: limit) == [.tooLarge(size: limit + 1, limit: limit)])
        #expect(ProduceValidator.defaultMaxMessageBytes == 1_048_576)
    }

    @Test func severalProblemsAreAllReported() {
        let issues = ProduceValidator.issues(for: request(topic: "", partition: -2))
        #expect(issues == [.emptyTopic, .negativePartition])
    }

    @Test func theErrorForIssuesKeepsTheRightCode() {
        #expect(ProduceValidator.error(for: []) == nil)
        guard case .clusterError(let tooLarge, _)? = ProduceValidator.error(for: [.tooLarge(size: 2, limit: 1)]) else { Issue.record("expected clusterError"); return }
        #expect(tooLarge == KafkaError.messageTooLargeCode)
        guard case .clusterError(let invalid, let text)? = ProduceValidator.error(for: [.emptyTopic]) else { Issue.record("expected clusterError"); return }
        #expect(invalid == KafkaError.invalidProduceRequestCode)
        #expect(ProduceValidator.error(for: [.emptyTopic])?.userMessage == text)
    }

    @Test func tooLargeHasItsOwnFriendlyMessage() {
        let message = KafkaError.clusterError(code: KafkaError.messageTooLargeCode, message: "MSG_SIZE_TOO_LARGE").userMessage
        #expect(message.contains("larger than the cluster accepts"))
        #expect(!message.contains("MSG_SIZE"))
    }

    @Test func aRequestCopiesAMessage() {
        let message = KafkaMessage(
            partition: 4, offset: 99, timestamp: Date(timeIntervalSince1970: 5), key: Data("k".utf8), value: nil,
            headers: [MessageHeader(key: "h", value: Data([1, 2]))], timestampType: .createTime)
        let copy = ProduceRequest(copying: message, to: "other")
        #expect(copy.topic == "other" && copy.partition == nil && copy.timestamp == nil)
        #expect(copy.key == message.key && copy.value == nil && copy.headers == message.headers)
        #expect(ProduceRequest(copying: message, to: "t", partition: 2).partition == 2)
    }
}

@Suite("ProduceEncoder")
struct ProduceEncoderTests {
    private func data(_ text: String, _ encoding: ProduceEncoding) -> Data? {
        if case .success(let data) = ProduceEncoder.data(from: text, as: encoding) { return data }
        return nil
    }

    @Test func textIsUTF8AsTyped() {
        #expect(data("héllo 🎉\n", .text) == Data("héllo 🎉\n".utf8))
        #expect(data("", .text) == Data())
    }

    @Test(arguments: [
        ("48656c6c6f", [0x48, 0x65, 0x6c, 0x6c, 0x6f]),
        ("48 65 6C 6c 6F", [0x48, 0x65, 0x6c, 0x6c, 0x6f]),
        ("0x4865", [0x48, 0x65]),
        ("  0X48:65-01,ff\n", [0x48, 0x65, 0x01, 0xff]),
        ("", []),
        ("   ", []),
        ("0x", []),
    ] as [(String, [UInt8])])
    func hexIsTolerantOfSeparatorsAndPrefix(text: String, bytes: [UInt8]) {
        #expect(data(text, .hex) == Data(bytes))
    }

    @Test func badHexIsExplained() {
        #expect(ProduceEncoder.data(from: "abc", as: .hex) == .failure(.oddHexLength))
        #expect(ProduceEncoder.data(from: "0x123", as: .hex) == .failure(.oddHexLength))
        #expect(ProduceEncoder.data(from: "12 zz", as: .hex) == .failure(.invalidHexDigit(offset: 3)))
        #expect(ProduceEncoder.data(from: "0xg0", as: .hex) == .failure(.invalidHexDigit(offset: 2)))
        #expect(ProduceEncoder.data(from: "١٢", as: .hex) == .failure(.invalidHexDigit(offset: 0)), "non-ASCII digits are not hex")
        for issue in [ProduceEncodingIssue.oddHexLength, .invalidHexDigit(offset: 1), .invalidJSON(offset: 3), .jsonTooDeep] { #expect(!issue.message.isEmpty) }
    }

    @Test func jsonKeepsTheTextExactlyAsTyped() {
        let text = "{ \"a\":1,\n  \"b\" : [true,null] }"
        #expect(data(text, .json) == Data(text.utf8), "validated, not rewritten")
        #expect(data("42", .json) == Data("42".utf8))
        #expect(data("\"x\"", .json) != nil)
    }

    @Test func invalidJSONIsRejected() {
        for text in ["", "{", "{\"a\":}", "[1,]", "nope", "{'a':1}", "{\"a\":1} extra"] {
            guard case .failure(.invalidJSON)? = Optional(ProduceEncoder.data(from: text, as: .json)) else {
                Issue.record("expected invalidJSON for \(text)")
                continue
            }
        }
    }

    @Test func formatReindentsAndKeepsOrder() throws {
        let formatted = try ProduceEncoder.formatJSON("{\"b\":1,\"a\":[1,2]}").get()
        #expect(formatted == "{\n  \"b\": 1,\n  \"a\": [\n    1,\n    2\n  ]\n}")
        #expect(ProduceEncoder.formatJSON("{") == .failure(.invalidJSON(offset: 1)))
        // Formatting does not shrink a big document to its first lines.
        let many = "[" + (0..<2000).map(String.init).joined(separator: ",") + "]"
        #expect(try ProduceEncoder.formatJSON(many).get().contains("1999"))
    }

    @Test func existingBytesBecomeTextOrHex() {
        #expect(ProduceEncoder.editableText(for: Data("héllo".utf8)) == ("héllo", .text))
        #expect(ProduceEncoder.editableText(for: Data()) == ("", .text))
        let binary = ProduceEncoder.editableText(for: Data([0x00, 0xFF, 0x10]))
        #expect(binary.encoding == .hex && binary.text == "00 ff 10")
        #expect(data(binary.text, .hex) == Data([0x00, 0xFF, 0x10]), "what we show as hex parses back to the same bytes")
        #expect(ProduceEncoder.hexString(Data([0xAB])) == "ab")
        #expect(ProduceEncoder.editableText(for: Data([0x00, 0x01])).encoding == .hex, "valid UTF-8 that would be invisible")
        #expect(ProduceEncoder.editableText(for: Data("a\tb\r\nc".utf8)).encoding == .text)
        #expect(ProduceEncoder.editableText(for: Data([0x7F])).encoding == .hex)
    }
}

private let cluster = ClusterConnection(name: "fake", bootstrapServers: "h:9092")

private func orders() async throws -> FakeKafkaClient {
    let fake = FakeKafkaClient.sampleData(topicCount: 3)
    try await fake.connect(cluster, credentials: nil)
    return fake
}

@Suite("Fake produce")
struct FakeProduceTests {
    @Test func theMessageIsVisibleToTheNextFetchAtTheReturnedOffset() async throws {
        let fake = try await orders()
        let before = try await fake.describeTopic("orders").partitions[1].endOffset
        let result = try await fake.produce(ProduceRequest(
            topic: "orders", partition: 1, key: Data("k".utf8), value: Data("new".utf8), headers: [MessageHeader(key: "h", value: Data("1".utf8))]))
        #expect(result.partition == 1 && result.offset == before)
        #expect(try await fake.describeTopic("orders").partitions[1].endOffset == before + 1)
        var read: [KafkaMessage] = []
        for try await message in fake.fetchMessages(MessageQuery(topic: "orders", partition: 1, start: .offset(before), limit: 10)) { read.append(message) }
        #expect(read.count == 1)
        #expect(read.first?.value == Data("new".utf8) && read.first?.key == Data("k".utf8))
        #expect(read.first?.headers == [MessageHeader(key: "h", value: Data("1".utf8))])
    }

    @Test func tombstonesAndEmptyValuesStayDifferent() async throws {
        let fake = try await orders()
        let tomb = try await fake.produce(ProduceRequest(topic: "orders", partition: 0, value: nil))
        let empty = try await fake.produce(ProduceRequest(topic: "orders", partition: 0, value: Data()))
        #expect(empty.offset == tomb.offset + 1)
        var read: [KafkaMessage] = []
        for try await message in fake.fetchMessages(MessageQuery(topic: "orders", partition: 0, start: .offset(tomb.offset), limit: 10)) { read.append(message) }
        #expect(read.map(\.value) == [nil, Data()])
    }

    @Test func autoPartitionIsStableForAKeyAndSpreadsKeylessMessages() async throws {
        let fake = try await orders()
        let first = try await fake.produce(ProduceRequest(topic: "orders", key: Data("same".utf8), value: Data()))
        let second = try await fake.produce(ProduceRequest(topic: "orders", key: Data("same".utf8), value: Data()))
        #expect(first.partition == second.partition && second.offset == first.offset + 1)
        var partitions: Set<Int> = []
        for _ in 0..<6 { partitions.insert(try await fake.produce(ProduceRequest(topic: "orders", value: Data())).partition) }
        #expect(partitions == [0, 1, 2])
    }

    @Test func theTimestampIsKeptOrNow() async throws {
        let fake = try await orders()
        let when = Date(timeIntervalSince1970: 1_600_000_000)
        #expect(try await fake.produce(ProduceRequest(topic: "orders", partition: 0, value: Data(), timestamp: when)).timestamp == when)
        #expect(abs(try await fake.produce(ProduceRequest(topic: "orders", partition: 0, value: Data())).timestamp.timeIntervalSinceNow) < 5)
    }

    @Test func unknownTopicAndPartitionAreErrorsAndNothingIsCreated() async throws {
        let fake = try await orders()
        await #expect(throws: KafkaError.unknownTopic("nope")) { _ = try await fake.produce(ProduceRequest(topic: "nope", value: Data())) }
        await #expect(throws: KafkaError.unknownTopic("orders")) { _ = try await fake.produce(ProduceRequest(topic: "orders", partition: 9, value: Data())) }
        #expect(try await fake.listTopics().contains { $0.name == "nope" } == false)
    }

    @Test func invalidRequestsAreRefusedLikeTheLiveClient() async throws {
        let fake = try await orders()
        await #expect(throws: (any Error).self) { _ = try await fake.produce(ProduceRequest(topic: "orders", value: Data(count: ProduceValidator.defaultMaxMessageBytes + 1))) }
        await #expect(throws: (any Error).self) { _ = try await fake.produce(ProduceRequest(topic: "orders", headers: [MessageHeader(key: "", value: nil)])) }
        #expect(try await fake.describeTopic("orders").partitions.map(\.endOffset) == [30, 30, 30])
    }

    @Test func failuresCanBeInjectedAndNeedAConnection() async throws {
        let fake = try await orders()
        await fake.failing(.produce, with: .timeout)
        await #expect(throws: KafkaError.timeout) { _ = try await fake.produce(ProduceRequest(topic: "orders", value: Data())) }
        await fake.failing(.produce, with: nil)
        await fake.failNext(.authorizationFailed(resource: "orders"))
        await #expect(throws: KafkaError.authorizationFailed(resource: "orders")) { _ = try await fake.produce(ProduceRequest(topic: "orders", value: Data())) }
        let idle = FakeKafkaClient.sampleData(topicCount: 3)
        await #expect(throws: KafkaError.self) { _ = try await idle.produce(ProduceRequest(topic: "orders", value: Data())) }
    }
}
