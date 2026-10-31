import Testing
import Foundation
import os
import KafkaManagerCore
@testable import KafkaManagerUI

private let typedPassword = "typed-pw"
private let savedPassword = "saved-pw"

/// Counts connects and disconnects around a fake; everything else forwards.
private final class RecordingClient: KafkaClient, Sendable {
    let wrapped: FakeKafkaClient
    private let counts = OSAllocatedUnfairLock(initialState: (connects: 0, disconnects: 0))

    init(_ wrapped: FakeKafkaClient) { self.wrapped = wrapped }

    var connects: Int { counts.withLock { $0.connects } }
    var disconnects: Int { counts.withLock { $0.disconnects } }

    func connect(_ cluster: ClusterConnection, credentials: Credentials?) async throws {
        counts.withLock { $0.connects += 1 }
        try await wrapped.connect(cluster, credentials: credentials)
    }
    func disconnect() async {
        counts.withLock { $0.disconnects += 1 }
        await wrapped.disconnect()
    }
    func describeCluster() async throws -> ClusterInfo { try await wrapped.describeCluster() }
    func listTopics() async throws -> [TopicSummary] { try await wrapped.listTopics() }
    func describeTopic(_ name: String) async throws -> TopicDetail { try await wrapped.describeTopic(name) }
    func createTopic(_ spec: NewTopic) async throws { try await wrapped.createTopic(spec) }
    func deleteTopic(_ name: String) async throws { try await wrapped.deleteTopic(name) }
    func fetchMessages(_ query: MessageQuery) -> AsyncThrowingStream<KafkaMessage, Error> { wrapped.fetchMessages(query) }
    func listGroups() async throws -> [GroupSummary] { try await wrapped.listGroups() }
    func describeGroup(_ id: String) async throws -> GroupDetail { try await wrapped.describeGroup(id) }
    func previewOffsetReset(group: String, target: OffsetResetTarget, scope: OffsetResetScope) async throws -> OffsetResetPreview {
        try await wrapped.previewOffsetReset(group: group, target: target, scope: scope)
    }
    func resetOffsets(group: String, plan: OffsetResetPlan) async throws -> OffsetResetResult { try await wrapped.resetOffsets(group: group, plan: plan) }
}

private func profile() -> ClusterConnection {
    ClusterConnection(name: "c", bootstrapServers: "h:9092")
}

// MARK: ClientConnectionTester

@Suite("ClientConnectionTester")
struct ClientConnectionTesterTests {
    private func tester(_ client: RecordingClient, sleeper: SleepController = SleepController()) -> ClientConnectionTester {
        ClientConnectionTester(factory: { _ in client }, timeout: .seconds(10), sleep: sleeper.sleep)
    }

    @Test func successReportsBrokerCountAndDisconnects() async throws {
        let client = RecordingClient(FakeKafkaClient(brokerCount: 4))
        let count = try await tester(client).test(profile(), credentials: nil)
        #expect(count == 4)
        #expect(client.connects == 1)
        #expect(client.disconnects == 1)
    }

    @Test func unreachableThrowsAndStillDisconnects() async {
        let fake = FakeKafkaClient()
        await fake.failNext(.unreachable(host: "h:9092"))
        let client = RecordingClient(fake)
        await #expect(throws: KafkaError.unreachable(host: "h:9092")) { _ = try await tester(client).test(profile(), credentials: nil) }
        #expect(client.disconnects == 1)
    }

    @Test func wrongCredentialsFailAuthentication() async throws {
        let fake = FakeKafkaClient(expectedCredentials: Credentials(username: "a", password: "right"))
        let client = RecordingClient(fake)
        await #expect(throws: KafkaError.authenticationFailed) {
            _ = try await tester(client).test(profile(), credentials: Credentials(username: "a", password: "wrong"))
        }
        let ok = RecordingClient(fake)
        #expect(try await tester(ok).test(profile(), credentials: Credentials(username: "a", password: "right")) == 3)
        #expect(client.disconnects == 1 && ok.disconnects == 1)
    }

    @Test func timeoutFiresAfterTheInjectedSleepAndDisconnects() async throws {
        let sleeper = SleepController()
        let client = RecordingClient(FakeKafkaClient(latency: .seconds(1_000)))
        let subject = tester(client, sleeper: sleeper)
        let task = Task { try await subject.test(profile(), credentials: nil) }
        await expectSettles { await sleeper.pendingCount == 1 }
        #expect(await sleeper.requested == [.seconds(10)])
        await sleeper.fire(0)
        await #expect(throws: KafkaError.timeout) { _ = try await task.value }
        #expect(client.disconnects == 1)
    }

    @Test func cancellationStopsTheTestAndStillDisconnects() async {
        let sleeper = SleepController()
        let client = RecordingClient(FakeKafkaClient(latency: .seconds(1_000)))
        let subject = tester(client, sleeper: sleeper)
        let task = Task { try await subject.test(profile(), credentials: nil) }
        await expectSettles { await sleeper.pendingCount == 1 }
        task.cancel()
        await #expect(throws: CancellationError.self) { _ = try await task.value }
        #expect(client.disconnects == 1)
    }
}

// MARK: Form view model

private actor GatedTester: ConnectionTester {
    private let gate = Gate()
    private let outcome: Result<Int, any Error>
    private(set) var seen: [(ClusterConnection, Credentials?)] = []

    init(outcome: Result<Int, any Error> = .success(3)) { self.outcome = outcome }

    func test(_ connection: ClusterConnection, credentials: Credentials?) async throws -> Int {
        seen.append((connection, credentials))
        await gate.wait()
        return try outcome.get()
    }

    func release() async { await gate.open() }
    var credentials: [Credentials?] { seen.map(\.1) }
}

private struct UnknownFailure: Error {}

@MainActor
private func makeForm(
    mode: ConnectionFormViewModel.Mode = .new,
    tester: (any ConnectionTester)?,
    secrets: any SecretStore = InMemorySecretStore(),
    store: any ConnectionStore = InMemoryConnectionStore(),
    sasl: Bool = false
) -> ConnectionFormViewModel {
    let form = ConnectionFormViewModel(mode: mode, store: store, secrets: secrets, existingNames: [], tester: tester)
    if case .new = mode {
        form.name = "  prod "
        form.bootstrapServers = " b1 , b2:9093 "
    }
    if sasl {
        form.security = .saslSsl
        form.saslMechanism = .plain
        form.username = " alice "
    }
    return form
}

@MainActor
private func runTest(_ form: ConnectionFormViewModel, tester: GatedTester) async {
    let task = Task { @MainActor in await form.testConnection() }
    await expectSettles { form.testState == .testing || form.testState != .idle }
    await tester.release()
    await task.value
}

@Suite("Test connection (form)") @MainActor
struct ConnectionTestFormTests {
    @Test func successShowsBrokerCountAndUsesTheDraftValues() async {
        let tester = GatedTester(outcome: .success(5))
        let form = makeForm(tester: tester)
        await runTest(form, tester: tester)
        #expect(form.testState == .success(brokerCount: 5))
        #expect(form.testState.message == "Connected. 5 broker(s).")
        let seen = await tester.seen
        #expect(seen.first?.0.bootstrapServers == "b1:9092,b2:9093")
        #expect(seen.first?.0.name == "prod")
        #expect(seen.first?.1 == nil)
    }

    @Test func kafkaErrorsShowTheirUserMessage() async {
        for error in [KafkaError.unreachable(host: "b1:9092"), .authenticationFailed, .timeout] {
            let tester = GatedTester(outcome: .failure(error))
            let form = makeForm(tester: tester)
            await runTest(form, tester: tester)
            #expect(form.testState == .failure(message: error.userMessage))
            #expect(form.testState.message == error.userMessage)
        }
    }

    @Test func unknownErrorsGetAGenericMessage() async {
        let tester = GatedTester(outcome: .failure(UnknownFailure()))
        let form = makeForm(tester: tester)
        await runTest(form, tester: tester)
        #expect(form.testState == .failure(message: String(localized: "The connection test failed.")))
    }

    @Test func realTesterEndToEndAgainstTheFake() async {
        let fake = FakeKafkaClient(expectedCredentials: Credentials(username: "alice", password: typedPassword), brokerCount: 2)
        let tester = ClientConnectionTester(factory: { _ in fake })
        let form = makeForm(tester: tester, sasl: true)
        form.password = typedPassword
        await form.testConnection()
        #expect(form.testState == .success(brokerCount: 2))
    }

    // MARK: Availability

    @Test func cannotTestWithoutATester() {
        let form = makeForm(tester: nil)
        #expect(!form.canTest)
    }

    @Test func disabledWhileTheDraftIsInvalid() {
        let form = makeForm(tester: GatedTester())
        #expect(form.canTest)
        form.bootstrapServers = "bad host"
        #expect(!form.canTest)
        form.bootstrapServers = "h"
        form.name = ""
        #expect(!form.canTest)
        form.name = "x"
        form.security = .saslSsl
        #expect(!form.canTest)  // missing user name and mechanism
    }

    @Test func warningsDoNotBlockTesting() {
        let form = ConnectionFormViewModel(mode: .new, store: InMemoryConnectionStore(), secrets: InMemorySecretStore(),
                                           existingNames: ["Prod"], tester: GatedTester())
        form.name = "prod"
        form.bootstrapServers = "a,a"
        #expect(form.canTest)
    }

    @Test func disabledWhileTesting() async {
        let tester = GatedTester()
        let form = makeForm(tester: tester)
        let task = Task { @MainActor in await form.testConnection() }
        await expectSettles { form.testState == .testing }
        #expect(!form.canTest)
        await form.testConnection()  // ignored: a test is already running
        #expect(await tester.seen.count == 1)
        await tester.release()
        await task.value
        #expect(form.canTest)
    }

    // MARK: Cancel, stale, reset

    @Test func cancellingTheTaskReturnsToIdle() async {
        let tester = GatedTester()
        let form = makeForm(tester: tester)
        let task = Task { @MainActor in await form.testConnection() }
        await expectSettles { form.testState == .testing }
        task.cancel()
        await tester.release()
        await task.value
        #expect(form.testState == .idle)
    }

    @Test func editingAnyFieldClearsTheResult() async {
        let edits: [(String, @MainActor (ConnectionFormViewModel) -> Void)] = [
            ("name", { $0.name = "other" }),
            ("bootstrap", { $0.bootstrapServers = "z:1" }),
            ("environment", { $0.environment = .staging }),
            ("security", { $0.security = .ssl }),
            ("mechanism", { $0.saslMechanism = .scramSha256 }),
            ("username", { $0.username = "bob" }),
            ("password", { $0.password = "x" }),
            ("clear", { $0.clearStoredPassword = true }),
        ]
        for (label, edit) in edits {
            let tester = GatedTester()
            let form = makeForm(tester: tester)
            await runTest(form, tester: tester)
            #expect(form.testState == .success(brokerCount: 3), Comment(rawValue: label))
            edit(form)
            #expect(form.testState == .idle, Comment(rawValue: label))
        }
    }

    @Test func aStaleResultIsIgnoredAfterAnEdit() async {
        let tester = GatedTester()
        let form = makeForm(tester: tester)
        let task = Task { @MainActor in await form.testConnection() }
        await expectSettles { form.testState == .testing }
        form.name = "edited"
        #expect(form.testState == .idle)
        await tester.release()
        await task.value
        #expect(form.testState == .idle)
    }

    @Test func aNewTestWinsOverAnOlderStaleOne() async {
        let slow = GatedTester(outcome: .failure(KafkaError.timeout))
        let form = makeForm(tester: slow)
        let old = Task { @MainActor in await form.testConnection() }
        await expectSettles { form.testState == .testing }
        form.name = "edited"  // invalidates the first test
        let second = Task { @MainActor in await form.testConnection() }
        await expectSettles { await slow.seen.count == 2 }
        await slow.release()
        await old.value
        await second.value
        #expect(form.testState == .failure(message: KafkaError.timeout.userMessage))
    }

    // MARK: Passwords

    @Test func typedPasswordWinsOverTheSavedOne() async throws {
        let existing = ClusterConnection(name: "s", bootstrapServers: "h:9093", security: .saslSsl, saslMechanism: .plain, username: "alice")
        let secrets = InMemorySecretStore()
        try await secrets.setPassword(savedPassword, for: existing.id)
        let tester = GatedTester()
        let form = makeForm(mode: .edit(existing), tester: tester, secrets: secrets)
        form.password = typedPassword
        await runTest(form, tester: tester)
        #expect(await tester.credentials == [Credentials(username: "alice", password: typedPassword)])
    }

    @Test func savedPasswordIsReadAtTestTimeWhenNothingTyped() async throws {
        let existing = ClusterConnection(name: "s", bootstrapServers: "h:9093", security: .saslSsl, saslMechanism: .plain, username: "alice")
        let secrets = SpySecretStore()
        try await secrets.setPassword(savedPassword, for: existing.id)
        let tester = GatedTester()
        let form = makeForm(mode: .edit(existing), tester: tester, secrets: secrets)
        let before = await secrets.callCount
        await runTest(form, tester: tester)
        #expect(await tester.credentials == [Credentials(username: "alice", password: savedPassword)])
        #expect(await secrets.callCount == before + 1)
    }

    @Test func missingPasswordIsAnEmptyOne() async {
        let tester = GatedTester()
        let form = makeForm(tester: tester, sasl: true)
        await runTest(form, tester: tester)
        #expect(await tester.credentials == [Credentials(username: "alice", password: "")])
    }

    @Test func removingTheSavedPasswordMeansTestingWithoutIt() async throws {
        let existing = ClusterConnection(name: "s", bootstrapServers: "h:9093", security: .saslSsl, saslMechanism: .plain, username: "alice")
        let secrets = InMemorySecretStore()
        try await secrets.setPassword(savedPassword, for: existing.id)
        let tester = GatedTester()
        let form = makeForm(mode: .edit(existing), tester: tester, secrets: secrets)
        form.clearStoredPassword = true
        await runTest(form, tester: tester)
        #expect(await tester.credentials == [Credentials(username: "alice", password: "")])
    }

    @Test func plaintextTestsNeverReadSecretsOrSendCredentials() async {
        let secrets = SpySecretStore()
        let tester = GatedTester()
        let form = makeForm(tester: tester, secrets: secrets)
        await runTest(form, tester: tester)
        #expect(await secrets.callCount == 0)
        #expect(await tester.credentials.allSatisfy { $0 == nil })
    }

    @Test func passwordsNeverAppearInStateOrMessages() async throws {
        let fake = FakeKafkaClient(expectedCredentials: Credentials(username: "alice", password: "other"))
        let form = makeForm(tester: ClientConnectionTester(factory: { _ in fake }), sasl: true)
        form.password = typedPassword
        await form.testConnection()
        #expect(form.testState == .failure(message: KafkaError.authenticationFailed.userMessage))
        for text in [String(describing: form.testState), String(reflecting: form.testState), form.testState.message ?? ""] {
            #expect(!text.contains(typedPassword))
        }
    }

    @Test func testingWritesNothing() async {
        let log = EventLog()
        let store = SpyConnectionStore(log: log)
        let secrets = SpySecretStore(log: log)
        let tester = GatedTester()
        let form = makeForm(tester: tester, secrets: secrets, store: store, sasl: true)
        form.password = typedPassword
        await runTest(form, tester: tester)
        #expect(await log.events.isEmpty)
        #expect(await store.callCount == 0)
        #expect(form.password == typedPassword)  // still in the field, untouched
    }
}
