import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

@MainActor
private struct ProduceRig {
    let fake: FakeKafkaClient
    let session: ClusterSession
    let sent = SentLog()

    init(environment: ClusterConnection.Environment = .local, latency: Duration = .zero) {
        let fake = FakeKafkaClient.sampleData(topicCount: 3, latency: latency)
        self.fake = fake
        session = ClusterSession(
            connection: ClusterConnection(name: "dev-cluster", bootstrapServers: "h:9092", environment: environment),
            secrets: InMemorySecretStore(), factory: { _ in fake })
    }

    func form(prefill: KafkaMessage? = nil) -> ProduceFormViewModel {
        let sent = sent
        return ProduceFormViewModel(
            session: session, topicName: "orders", partitionIDs: [0, 1, 2], prefill: prefill, onSent: { sent.results.append($0) })
    }

    func end(_ partition: Int) async throws -> Int64 {
        _ = try await session.ensureConnected()
        return try await fake.describeTopic("orders").partitions[partition].endOffset
    }

    func waitUntil(_ condition: () -> Bool) async {
        for _ in 0..<400 where !condition() { try? await Task.sleep(for: .milliseconds(10)) }
    }
}

@MainActor
private final class SentLog {
    var results: [ProduceResult] = []
}

@Suite("Produce form: fields") @MainActor
struct ProduceFormFieldTests {
    @Test func anEmptyFormSendsAnEmptyValueWithNoKey() {
        let form = ProduceRig().form()
        #expect(form.issues.isEmpty && form.canSend)
        #expect(form.request == ProduceRequest(topic: "orders", partition: nil, key: nil, value: Data(), headers: [], timestamp: nil))
    }

    @Test func keyAndValueFollowTheirEncodings() {
        let form = ProduceRig().form()
        form.keyText = "0x6b31"
        form.keyEncoding = .hex
        form.valueText = "{\"a\":1}"
        form.valueEncoding = .json
        form.partition = 2
        #expect(form.request?.key == Data("k1".utf8))
        #expect(form.request?.value == Data("{\"a\":1}".utf8))
        #expect(form.request?.partition == 2)
        form.keyEncoding = .text
        #expect(form.request?.key == Data("0x6b31".utf8), "switching the encoding re-reads the same text")
    }

    @Test func badHexAndBadJSONAreReportedOnTheirOwnField() {
        let form = ProduceRig().form()
        form.keyText = "abc"
        form.keyEncoding = .hex
        form.valueText = "{"
        form.valueEncoding = .json
        #expect(form.issues(for: .key).count == 1 && form.issues(for: .value).count == 1)
        #expect(form.request == nil && !form.canSend)
        #expect(form.sendDisabledReason != nil)
        form.keyText = "ab"
        form.valueText = "{}"
        #expect(form.issues.isEmpty && form.canSend)
    }

    @Test func aTombstoneHasNoValueAndIgnoresWhatWasTyped() {
        let form = ProduceRig().form()
        form.valueText = "{ not json"
        form.valueEncoding = .json
        #expect(!form.canSend)
        form.isTombstone = true
        #expect(form.canSend && form.request?.value == nil)
        form.isTombstone = false
        form.valueText = ""
        form.valueEncoding = .text
        #expect(form.request?.value == Data(), "an empty value is not a tombstone")
    }

    @Test func formatReindentsOnlyValidJSON() {
        let form = ProduceRig().form()
        form.valueEncoding = .json
        form.valueText = "{\"a\":[1,2]}"
        #expect(form.canFormatValue)
        form.formatValue()
        #expect(form.valueText == "{\n  \"a\": [\n    1,\n    2\n  ]\n}")
        form.valueText = "{"
        #expect(!form.canFormatValue)
        form.formatValue()
        #expect(form.valueText == "{")
        form.valueEncoding = .text
        form.valueText = "{\"a\":1}"
        form.formatValue()
        #expect(form.valueText == "{\"a\":1}", "only the JSON encoding formats")
    }

    @Test func headersBlankRowsAreIgnoredAndErrorsNameTheRow() {
        let form = ProduceRig().form()
        form.headers = [
            HeaderDraft(key: "trace", value: "abc"),
            HeaderDraft(),
            HeaderDraft(key: "", value: "orphan"),
            HeaderDraft(key: "raw", value: "00 ff", kind: .hex),
            HeaderDraft(key: "nothing", kind: .null),
        ]
        #expect(form.issues(for: .headers).map(\.message) == ["Header 3 needs a name."])
        form.headers[2].key = "third"
        #expect(form.request?.headers == [
            MessageHeader(key: "trace", value: Data("abc".utf8)), MessageHeader(key: "third", value: Data("orphan".utf8)),
            MessageHeader(key: "raw", value: Data([0, 255])), MessageHeader(key: "nothing", value: nil),
        ])
        form.headers[3].value = "zz"
        #expect(form.issues(for: .headers).first?.message.hasPrefix("Header 4") == true)
        form.removeHeader(form.headers[3].id)
        form.addHeader()
        #expect(form.headers.count == 5 && form.headers.last?.key == "")
    }

    @Test func sizeIsShownAndTheLimitBlocksSending() {
        let form = ProduceRig().form()
        form.valueText = String(repeating: "a", count: 12 * 1024)
        #expect(form.sizeText == "12 KB of 1 MB")
        #expect(!form.isOverSizeLimit && form.canSend)
        form.valueText = String(repeating: "a", count: ProduceValidator.defaultMaxMessageBytes)
        #expect(form.canSend, "exactly the limit still goes")
        form.valueText.append("a")
        #expect(form.isOverSizeLimit && !form.canSend)
        #expect(form.issues(for: .value).count == 1)
        form.isTombstone = true
        #expect(form.canSend)
    }

    @Test func timestampAndRepeatAreChecked() {
        let form = ProduceRig().form()
        form.useTimestamp = true
        form.timestamp = Date(timeIntervalSince1970: 1_700_000_000)
        #expect(form.request?.timestamp == Date(timeIntervalSince1970: 1_700_000_000))
        form.timestamp = Date(timeIntervalSince1970: -10)
        #expect(form.issues(for: .timestamp).count == 1 && !form.canSend)
        form.useTimestamp = false
        #expect(form.canSend && form.request?.timestamp == nil)
        for bad in [0, -1, ProduceFormViewModel.maxRepeat + 1] {
            form.repeatCount = bad
            #expect(form.issues(for: .repeatCount).count == 1 && !form.canSend)
        }
        form.repeatCount = ProduceFormViewModel.maxRepeat
        #expect(form.canSend)
    }

    @Test func aPartitionThatDoesNotExistIsRefused() {
        let form = ProduceRig().form()
        form.partition = 7
        #expect(form.issues(for: .partition).count == 1 && !form.canSend)
        form.partition = 1
        #expect(form.canSend)
    }
}

@Suite("Produce form: prefill") @MainActor
struct ProduceFormPrefillTests {
    @Test func textKeyValueAndHeadersAreCopiedAndThePartitionStaysAuto() {
        let message = KafkaMessage(
            partition: 2, offset: 7, timestamp: Date(), key: Data("k".utf8), value: Data("héllo".utf8),
            headers: [MessageHeader(key: "a", value: Data("1".utf8)), MessageHeader(key: "b", value: nil), MessageHeader(key: "c", value: Data([0xFF]))])
        let form = ProduceRig().form(prefill: message)
        #expect(form.keyText == "k" && form.keyEncoding == .text)
        #expect(form.valueText == "héllo" && form.valueEncoding == .text && !form.isTombstone)
        #expect(form.partition == nil && form.originalPartition == 2)
        #expect(form.headers.map(\.kind) == [.text, .null, .hex])
        #expect(form.headers.map(\.value) == ["1", "", "ff"])
        #expect(form.request == ProduceRequest(copying: message, to: "orders"))
    }

    @Test func binaryBecomesHexAndANullValueBecomesATombstone() {
        let binary = KafkaMessage(partition: 0, offset: 1, timestamp: Date(), key: Data([0, 1]), value: Data([0xC0, 0xFF]))
        let form = ProduceRig().form(prefill: binary)
        #expect(form.keyEncoding == .hex && form.valueEncoding == .hex && form.valueText == "c0 ff")
        #expect(form.request?.value == Data([0xC0, 0xFF]) && form.request?.key == Data([0, 1]))
        let tombstone = ProduceRig().form(prefill: KafkaMessage(partition: 0, offset: 2, timestamp: Date(), key: nil, value: nil))
        #expect(tombstone.isTombstone && tombstone.request?.value == nil && tombstone.request?.key == nil)
    }
}

@Suite("Produce form: sending") @MainActor
struct ProduceFormSendTests {
    @Test func aSuccessfulSendReportsWhereItLandedAndShowsInTheNextFetch() async throws {
        let rig = ProduceRig()
        let form = rig.form()
        form.partition = 1
        form.keyText = "k"
        form.valueText = "hello"
        let before = try await rig.end(1)
        await form.send()
        guard case .sent(let summary) = form.state else { Issue.record("expected sent, got \(form.state)"); return }
        #expect(summary.text == "Sent to partition 1 at offset \(before)")
        #expect(rig.sent.results == [summary.last])
        var read: [KafkaMessage] = []
        for try await message in rig.fake.fetchMessages(MessageQuery(topic: "orders", partition: 1, start: .offset(before), limit: 5)) { read.append(message) }
        #expect(read.map(\.value) == [Data("hello".utf8)])
        #expect(form.progress == nil)
    }

    @Test func sendAnotherKeepsTheFieldsAndSendsAgain() async throws {
        let rig = ProduceRig()
        let form = rig.form()
        form.partition = 0
        form.valueText = "again"
        await form.send()
        form.sendAnother()
        #expect(form.state == .idle && form.valueText == "again" && form.partition == 0)
        await form.send()
        #expect(rig.sent.results.map(\.offset) == [30, 31])
    }

    @Test func aFailureKeepsEverythingAndSaysWhy() async throws {
        let rig = ProduceRig()
        let form = rig.form()
        form.valueText = "keep me"
        await rig.fake.failing(.produce, with: .authorizationFailed(resource: "orders"))
        await form.send()
        #expect(form.state == .failed(message: KafkaError.authorizationFailed(resource: "orders").userMessage))
        #expect(form.valueText == "keep me" && form.canSend && rig.sent.results.isEmpty)
        form.dismissFailure()
        #expect(form.state == .idle)
        await rig.fake.failing(.produce, with: nil)
        await form.send()
        if case .sent = form.state {} else { Issue.record("a retry after the failure should succeed") }
    }

    @Test func aTopicDeletedUnderUsIsReported() async throws {
        let rig = ProduceRig()
        let form = rig.form()
        try await rig.session.ensureConnected().deleteTopic("orders")
        await form.send()
        #expect(form.state == .failed(message: KafkaError.unknownTopic("orders").userMessage))
    }

    @Test func doubleSubmitSendsOnce() async throws {
        let rig = ProduceRig(latency: .milliseconds(80))
        let form = rig.form()
        form.partition = 0
        form.confirmSend()
        form.confirmSend()
        form.requestSend()
        await rig.waitUntil { if case .sent = form.state { true } else { false } }
        try await Task.sleep(for: .milliseconds(300))
        #expect(rig.sent.results.count == 1)
        #expect(try await rig.end(0) == 31)
    }

    @Test func repeatSendsEachCopyInOrderAndStopEndsEarly() async throws {
        let rig = ProduceRig()
        let form = rig.form()
        form.partition = 2
        form.repeatCount = 5
        await form.send()
        guard case .sent(let summary) = form.state else { Issue.record("expected sent"); return }
        #expect(summary.sent == 5 && !summary.wasStopped && summary.last.offset == 34)
        #expect(summary.text.hasPrefix("Sent 5 messages"))

        let slow = ProduceRig(latency: .milliseconds(60))
        let long = slow.form()
        long.partition = 0
        long.repeatCount = 200
        long.confirmSend()
        await slow.waitUntil { (long.progress?.done ?? 0) >= 2 }
        long.stop()
        await slow.waitUntil { if case .sent = long.state { true } else { false } }
        guard case .sent(let stopped) = long.state else { Issue.record("expected a stopped summary, got \(long.state)"); return }
        #expect(stopped.wasStopped && stopped.sent >= 2 && stopped.sent < 200)
        #expect(try await slow.end(0) == 30 + Int64(stopped.sent))
    }

    @Test func aFailurePartWayThroughSaysHowManyWentOut() async throws {
        let rig = ProduceRig(latency: .milliseconds(60))
        let form = rig.form()
        form.partition = 0
        form.repeatCount = 50
        form.confirmSend()
        await rig.waitUntil { (form.progress?.done ?? 0) >= 2 }
        await rig.fake.failing(.produce, with: .timeout)
        await rig.waitUntil { if case .failed = form.state { true } else { false } }
        guard case .failed(let message) = form.state else { Issue.record("expected failure"); return }
        #expect(message.hasPrefix("Sent ") && message.contains("of 50") && message.contains(KafkaError.timeout.userMessage))
        #expect(rig.sent.results.count == 1, "the sheet's owner still hears about the messages that did go out")
    }
}

@Suite("Produce form: safety") @MainActor
struct ProduceFormSafetyTests {
    @Test func aReadOnlyClusterOffersNothingAndWritesNothing() async throws {
        let rig = ProduceRig(environment: .production)
        let form = rig.form()
        #expect(form.writeGate.isReadOnly && !form.canSend)
        #expect(form.sendDisabledReason == "This cluster is read-only")
        form.requestSend()
        #expect(form.pendingConfirmation == nil)
        await form.send()
        #expect(form.state == .idle && rig.sent.results.isEmpty)
        #expect(try await rig.end(0) == 30)
    }

    @Test func productionAsksForTheTopicNameBeforeAnythingIsSent() async throws {
        let rig = ProduceRig(environment: .production)
        await rig.session.setReadOnly(false)
        let form = rig.form()
        form.partition = 0
        form.repeatCount = 3
        form.requestSend()
        let confirmation = try #require(form.pendingConfirmation)
        #expect(confirmation.objectName == "orders" && confirmation.clusterName == "dev-cluster" && confirmation.showsProductionBanner)
        #expect(confirmation.isSatisfied(by: "orders") && !confirmation.isSatisfied(by: "Orders") && !confirmation.isSatisfied(by: "order"))
        #expect(confirmation.consequences.contains("3 messages"))
        #expect(form.state == .idle && rig.sent.results.isEmpty, "nothing is sent until the confirmation is accepted")
        form.confirmSend()
        await rig.waitUntil { if case .sent = form.state { true } else { false } }
        #expect(rig.sent.results.count == 1)
        #expect(try await rig.end(0) == 33)
    }

    @Test func localAndStagingSendWithoutAnExtraConfirmation() async throws {
        for environment in [ClusterConnection.Environment.local, .staging] {
            let rig = ProduceRig(environment: environment)
            await rig.session.setReadOnly(false)
            let form = rig.form()
            form.requestSend()
            #expect(form.pendingConfirmation == nil)
            await rig.waitUntil { if case .sent = form.state { true } else { false } }
            #expect(rig.sent.results.count == 1, "\(environment)")
        }
    }

    @Test func readOnlyTurnedOnWhileTheSheetIsOpenBlocksTheSend() async throws {
        let rig = ProduceRig()
        let form = rig.form()
        await rig.session.setReadOnly(true)
        await form.send()
        #expect(!form.canSend && rig.sent.results.isEmpty)
    }
}

@Suite("Topic detail: produce") @MainActor
struct TopicProduceIntegrationTests {
    private func detail(_ rig: ProduceRig) -> TopicDetailViewModel {
        TopicDetailViewModel(session: rig.session, topicName: "orders")
    }

    @Test func requestProduceOpensTheSheetWithThePartitionsOfTheLoadedTopic() async {
        let rig = ProduceRig()
        let model = detail(rig)
        await model.loadIfNeeded()
        model.requestProduce()
        #expect(model.produceForm?.partitionIDs == [0, 1, 2] && model.produceForm?.topicName == "orders")
        let message = KafkaMessage(partition: 1, offset: 4, timestamp: Date(), key: nil, value: Data("x".utf8))
        model.requestProduce(prefill: message)
        #expect(model.produceForm?.originalPartition == 1 && model.produceForm?.valueText == "x")
    }

    @Test func aReadOnlyClusterDoesNotOpenTheSheet() async {
        let rig = ProduceRig(environment: .production)
        let model = detail(rig)
        await model.loadIfNeeded()
        #expect(model.writeGate.disabledReason == "This cluster is read-only")
        model.requestProduce()
        #expect(model.produceForm == nil)
    }

    @Test func aSendLeavesAHintOnlyWhenMessagesWereFetchedAndNeverFetchesByItself() async throws {
        let rig = ProduceRig()
        let model = detail(rig)
        await model.loadIfNeeded()
        model.requestProduce()
        await model.produceForm?.send()
        #expect(model.messages.hasNewMessage == false, "nothing was fetched yet, so nothing is stale")
        #expect(model.messages.rows.isEmpty)

        model.messages.partition = 0
        await model.messages.fetch()
        let shown = model.messages.rows.count
        #expect(model.messages.hasNewMessage == false)
        model.requestProduce()
        model.produceForm?.partition = 0
        await model.produceForm?.send()
        #expect(model.messages.hasNewMessage)
        #expect(model.messages.rows.count == shown, "the table is not refreshed behind the user's back")
        await model.messages.fetch()
        #expect(model.messages.hasNewMessage == false && model.messages.rows.count == shown + 1)
    }

    @Test func theEndOffsetsAreRefreshedAfterASend() async throws {
        let rig = ProduceRig()
        let model = detail(rig)
        await model.loadIfNeeded()
        model.requestProduce()
        model.produceForm?.partition = 2
        await model.produceForm?.send()
        await rig.waitUntil { model.state.value?.partitions[2].endOffset == 31 }
        #expect(model.state.value?.partitions[2].endOffset == 31)
    }
}
