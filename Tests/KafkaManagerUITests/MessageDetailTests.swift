import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

@MainActor
final class FakeClipboard: Clipboard {
    private(set) var copied: [String] = []
    func copy(_ text: String) { copied.append(text) }
}

private func msg(
    key: Data? = Data("k".utf8), value: Data? = Data(#"{"a":1}"#.utf8), headers: [MessageHeader] = [],
    partition: Int = 2, offset: Int64 = 7, at seconds: Double = 1_700_000_000.123
) -> KafkaMessage {
    KafkaMessage(partition: partition, offset: offset, timestamp: Date(timeIntervalSince1970: seconds), key: key, value: value, headers: headers)
}

private func jsonObject(_ text: String) throws -> [String: Any] {
    try #require(try JSONSerialization.jsonObject(with: Data(text.utf8)) as? [String: Any])
}

@Suite("MessageExport")
struct MessageExportTests {
    @Test func textPayloadsExportAsUTF8() throws {
        let object = try jsonObject(MessageExport.json(msg(), topic: "orders"))
        #expect(object["topic"] as? String == "orders")
        #expect(object["partition"] as? Int == 2)
        #expect(object["offset"] as? Int == 7)
        #expect(object["timestamp"] as? String == "2023-11-14T22:13:20.123Z")
        #expect(object["key"] as? String == "k" && object["keyEncoding"] as? String == "utf8")
        #expect(object["value"] as? String == #"{"a":1}"# && object["valueEncoding"] as? String == "utf8")
        #expect((object["headers"] as? [Any])?.isEmpty == true)
    }

    @Test func nullKeyAndValueAreJSONNullWithoutEncoding() throws {
        let object = try jsonObject(MessageExport.json(msg(key: nil, value: nil), topic: "t"))
        #expect(object["key"] is NSNull && object["value"] is NSNull)
        #expect(object["keyEncoding"] == nil && object["valueEncoding"] == nil)
    }

    @Test func emptyIsAnEmptyStringNotNull() throws {
        let object = try jsonObject(MessageExport.json(msg(key: Data(), value: Data()), topic: "t"))
        #expect(object["key"] as? String == "" && object["valueEncoding"] as? String == "utf8")
    }

    @Test func binaryIsBase64() throws {
        let bytes = Data([0xFF, 0x00, 0x10, 0x80])
        let object = try jsonObject(MessageExport.json(msg(value: bytes), topic: "t"))
        #expect(object["value"] as? String == bytes.base64EncodedString())
        #expect(object["valueEncoding"] as? String == "base64")
    }

    @Test func unicodeAndSlashesAreKeptAsIs() throws {
        let text = "Xin chào 😀 a/b \"q\" \n"
        let json = MessageExport.json(msg(value: Data(text.utf8)), topic: "t")
        #expect(json.contains("Xin chào 😀 a/b"))
        #expect(try jsonObject(json)["value"] as? String == text)
    }

    @Test func headersKeepOrderAndEncodePerValue() throws {
        let headers = [
            MessageHeader(key: "trace", value: Data("abc".utf8)),
            MessageHeader(key: "bin", value: Data([0xC3, 0x28])),
            MessageHeader(key: "none", value: nil),
        ]
        let object = try jsonObject(MessageExport.json(msg(headers: headers), topic: "t"))
        let list = try #require(object["headers"] as? [[String: Any]])
        #expect(list.map { $0["key"] as? String } == ["trace", "bin", "none"])
        #expect(list[0]["value"] as? String == "abc" && list[0]["encoding"] as? String == "utf8")
        #expect(list[1]["encoding"] as? String == "base64")
        #expect(list[2]["value"] is NSNull && list[2]["encoding"] == nil)
    }

    @Test func rawTextIsUTF8OrBase64OrNil() {
        #expect(MessageExport.rawText(nil) == nil)
        #expect(MessageExport.rawText(Data("héllo".utf8)) == "héllo")
        #expect(MessageExport.rawText(Data([0xFF])) == "/w==")
    }
}

@Suite("Display lines and metadata")
struct DisplayLinesTests {
    @Test func splitsAtNewlinesKeepingEmptyLines() {
        #expect(DisplayLines.split("a\n\nb").map(\.text) == ["a", "", "b"])
        #expect(DisplayLines.split("").isEmpty)
    }

    @Test func longLinesAreCutAndIdsAreSequential() {
        let lines = DisplayLines.split(String(repeating: "x", count: 2_500), maxLineLength: 1_000)
        #expect(lines.map(\.text.count) == [1_000, 1_000, 500])
        #expect(lines.map(\.id) == [0, 1, 2])
        #expect(DisplayLines.wasSplit(String(repeating: "x", count: 1_001)))
        #expect(!DisplayLines.wasSplit("short\nlines"))
    }

    @Test func cutsOnCharacterBoundaries() {
        let lines = DisplayLines.split(String(repeating: "😀", count: 5), maxLineLength: 2)
        #expect(lines.map(\.text) == ["😀😀", "😀😀", "😀"])
    }

    @Test func timestampTextShowsZoneAndEpochMillis() {
        let date = Date(timeIntervalSince1970: 1_700_000_000.123)
        let utc = MessageDetailViewModel.timestampText(date, timeZone: .gmt)
        #expect(utc == "2023-11-14T22:13:20.123Z (1700000000123)")
        let plus7 = MessageDetailViewModel.timestampText(date, timeZone: TimeZone(secondsFromGMT: 7 * 3600) ?? .gmt)
        #expect(plus7.hasPrefix("2023-11-15T05:13:20.123+07:00"))
    }

    @Test func sizeText() {
        #expect(MessageDetailViewModel.sizeText(nil) == "none")
        #expect(MessageDetailViewModel.sizeText(1) == "1 byte")
        #expect(MessageDetailViewModel.sizeText(1_234).hasSuffix("bytes"))
    }
}

/// Holds a decode until the test releases it.
private actor DecodeGate {
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private(set) var started = 0
    func wait() async {
        started += 1
        await withCheckedContinuation { waiters.append($0) }
    }
    func release(_ index: Int) { waiters[index].resume() }
}

@Suite("MessageDetailViewModel") @MainActor
struct MessageDetailViewModelTests {
    private func model(_ message: KafkaMessage, clipboard: FakeClipboard = FakeClipboard(), decode: MessageDetailViewModel.Decode? = nil) -> MessageDetailViewModel {
        MessageDetailViewModel(topic: "orders", message: message, clipboard: clipboard, decode: decode ?? MessageDetailViewModel.systemDecode)
    }

    @Test func autoShowsPrettyJSONAndFormatSwitchingRedecodes() async {
        let m = model(msg(value: Data(#"{"a":1,"b":[1,2]}"#.utf8)))
        await m.load(.value)
        #expect(m.value.payload?.kind == .json)
        #expect(m.value.lines.count > 1)
        m.setFormat(.text, for: .value)
        await m.load(.value)
        #expect(m.value.payload?.kind == .text)
        #expect(m.value.lines.map(\.text) == [#"{"a":1,"b":[1,2]}"#])
        m.setFormat(.hex, for: .value)
        await m.load(.value)
        #expect(m.value.payload?.kind == .hex)
    }

    @Test func tombstoneAndEmptyAndInvalidUTF8() async {
        let tomb = model(msg(key: nil, value: nil))
        await tomb.load(.value)
        await tomb.load(.key)
        #expect(tomb.value.payload?.displayText == "null (tombstone)")
        #expect(tomb.key.payload?.displayText == "null")
        let empty = model(msg(value: Data()))
        await empty.load(.value)
        #expect(empty.value.payload?.displayText == "(empty)")
        let bin = model(msg(value: Data([0xFF, 0xFE, 0x00])))
        await bin.load(.value)
        #expect(bin.value.payload?.kind == .hex)
        #expect(bin.value.payload?.notes.isEmpty == false)
    }

    @Test func largeValueIsTruncatedWithANoteAndDecodedAsync() async {
        let big = Data(String(repeating: "a", count: 200_000).utf8)
        let m = model(msg(value: big))
        await m.load(.value)
        #expect(m.value.payload?.isTruncated == true)
        #expect(m.value.payload?.notes.contains { $0.hasPrefix("Showing the first") } == true)
        #expect(m.value.payload?.byteCount == 200_000)
        #expect(m.value.lines.count == 66)
        #expect(m.value.wasSplit)
        #expect(!m.value.isDecoding)
    }

    @Test func staleSlowDecodeIsIgnored() async {
        let gate = DecodeGate()
        let big = Data(String(repeating: "a", count: 100_000).utf8)
        let slow: MessageDetailViewModel.Decode = { data, format, role, limit in
            await gate.wait()
            return await PayloadDecoder.decodeAsync(data, as: format, limit: limit, role: role)
        }
        let m = model(msg(value: big), decode: slow)
        m.setFormat(.text, for: .value)
        let first = Task { @MainActor in await m.load(.value) }
        await expectSettles { m.value.isDecoding }
        await expectSettles { await gate.started == 1 }
        m.setFormat(.hex, for: .value)
        let second = Task { @MainActor in await m.load(.value) }
        await expectSettles { await gate.started == 2 }
        await gate.release(1)
        await second.value
        #expect(m.value.payload?.kind == .hex)
        await gate.release(0)
        await first.value
        #expect(m.value.payload?.kind == .hex, "the older decode must not replace the newer one")
        #expect(!m.value.isDecoding)
    }

    @Test func copyValueCopiesTheFullPayloadNotJustTheShownPart() async {
        let clipboard = FakeClipboard()
        let text = String(repeating: "abc", count: 50_000)
        let m = model(msg(value: Data(text.utf8)), clipboard: clipboard)
        m.setFormat(.text, for: .value)
        await m.load(.value)
        #expect(m.value.payload?.isTruncated == true)
        await m.copy(.value)
        #expect(clipboard.copied == [text])
        #expect(m.lastCopy == .value)
    }

    @Test func copyFollowsTheChosenFormat() async {
        let clipboard = FakeClipboard()
        let m = model(msg(value: Data("hi".utf8)), clipboard: clipboard)
        m.setFormat(.hex, for: .value)
        await m.copy(.value)
        #expect(clipboard.copied.count == 1)
        #expect(clipboard.copied[0].contains("68 69"))
    }

    @Test func copyKeyAndNullAreHandled() async {
        let clipboard = FakeClipboard()
        let m = model(msg(key: Data("user-1".utf8), value: nil), clipboard: clipboard)
        #expect(m.canCopy(.key) && !m.canCopy(.value))
        await m.copy(.key)
        await m.copy(.value)
        #expect(clipboard.copied == ["user-1"])
    }

    @Test func copyMessageUsesTheJSONExport() {
        let clipboard = FakeClipboard()
        let message = msg(headers: [MessageHeader(key: "h", value: Data("v".utf8))])
        let m = model(message, clipboard: clipboard)
        m.copyMessage()
        #expect(clipboard.copied == [MessageExport.json(message, topic: "orders")])
        #expect(m.lastCopy == .message)
        m.setFormat(.hex, for: .key)
        #expect(m.lastCopy == nil)
    }

    @Test func timestampTypeRowAppearsOnlyWhenKnown() {
        let unknown = model(msg()).metadataRows.map(\.label)
        #expect(!unknown.contains("Timestamp type"))
        for type in [TimestampType.createTime, .logAppendTime, .notAvailable] {
            let message = KafkaMessage(partition: 0, offset: 0, timestamp: Date(timeIntervalSince1970: 0), key: nil, value: nil, timestampType: type)
            let rows = model(message).metadataRows
            #expect(rows.map(\.label).contains("Timestamp type"))
            #expect(rows.first { $0.label == "Timestamp type" }?.value == MessageDetailViewModel.timestampTypeText(type))
        }
    }

    @Test func headersAndMetadata() {
        let headers = [MessageHeader(key: "trace", value: Data("abc".utf8)), MessageHeader(key: "bin", value: Data([0xFF, 0x00])), MessageHeader(key: "n", value: nil)]
        let m = model(msg(headers: headers))
        #expect(m.headerRows.map(\.key) == ["trace", "bin", "n"])
        #expect(m.headerRows[0].value == "abc")
        #expect(m.headerRows[1].value.contains("FF") || m.headerRows[1].value.contains("ff"))
        let meta = Dictionary(uniqueKeysWithValues: m.metadataRows.map { ($0.label, $0.value) })
        #expect(meta["Topic"] == "orders" && meta["Partition"] == "2" && meta["Offset"] == "7")
        #expect(meta["Headers"] == "3" && meta["Key size"] == "1 byte")
    }

    @Test func rowContextMenuCopiesRawKeyAndValue() async {
        let clipboard = FakeClipboard()
        let messages = [msg(key: Data("k".utf8), value: Data([0xFF]), partition: 0, offset: 0), msg(key: nil, value: nil, partition: 0, offset: 1)]
        let topic = FakeKafkaClient.SeedTopic(
            detail: TopicDetail(name: "orders", partitions: [PartitionInfo(id: 0, leader: 1, replicas: [1], isr: [1], startOffset: 0, endOffset: 2)]),
            messages: messages)
        let fake = FakeKafkaClient(topics: [topic])
        let session = ClusterSession(
            connection: ClusterConnection(name: "dev", bootstrapServers: "h:9092", environment: .local),
            secrets: InMemorySecretStore(), factory: { _ in fake })
        let list = MessagesViewModel(session: session, topicName: "orders")
        list.startKind = .earliest
        await list.fetch()
        let first = MessageID(partition: 0, offset: 0), second = MessageID(partition: 0, offset: 1)
        list.copyRaw(.value, of: first, to: clipboard)
        list.copyRaw(.key, of: first, to: clipboard)
        list.copyRaw(.value, of: second, to: clipboard)
        #expect(clipboard.copied == ["/w==", "k"])
        #expect(list.canCopyRaw(.value, of: first) && !list.canCopyRaw(.value, of: second))
        list.selection = second
        #expect(list.selectedRow?.id == second)
    }
}
