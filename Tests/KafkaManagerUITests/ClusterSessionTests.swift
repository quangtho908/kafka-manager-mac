import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private let secretPassword = "s3cr3t-pw"

private func plain(_ environment: ClusterConnection.Environment = .local, servers: String = "h1:9092") -> ClusterConnection {
    ClusterConnection(name: "c", bootstrapServers: servers, environment: environment)
}

private func saslProfile() -> ClusterConnection {
    ClusterConnection(name: "sasl", bootstrapServers: "h:9093", environment: .staging, security: .saslSsl,
                      saslMechanism: .plain, username: "alice")
}

@MainActor
private func makeSession(
    _ connection: ClusterConnection = plain(),
    probe: FactoryProbe = FactoryProbe(),
    secrets: any SecretStore = InMemorySecretStore(),
    sleeper: SleepController = SleepController(),
    idle: Duration = .seconds(300)
) -> ClusterSession {
    ClusterSession(connection: connection, secrets: secrets, factory: probe.factory, idleTimeout: idle, sleep: sleeper.sleep)
}

private func newTopic() -> NewTopic {
    NewTopic(name: "spike-x", partitions: 1, replicationFactor: 1)
}

@Suite("ClusterSession") @MainActor
struct ClusterSessionTests {
    @Test func initCreatesNoClientAndReadsNoSecret() async {
        let probe = FactoryProbe()
        let secrets = SpySecretStore()
        let session = makeSession(saslProfile(), probe: probe, secrets: secrets)
        #expect(session.status == .idle)
        #expect(probe.callCount == 0)
        #expect(await secrets.callCount == 0)
        #expect(throws: ClusterSessionError.notConnected) { try session.client() }
    }

    @Test func connectSucceedsWithOneFactoryCall() async throws {
        let probe = FactoryProbe()
        let session = makeSession(probe: probe)
        await session.connect()
        #expect(session.status == .connected)
        #expect(probe.callCount == 1)
        _ = try session.client()
        await session.connect()
        #expect(probe.callCount == 1)
    }

    @Test func failedConnectReportsUserMessageAndALaterConnectRetries() async throws {
        let fake = FakeKafkaClient()
        await fake.failNext(.unreachable(host: "h1:9092"))
        let session = ClusterSession(connection: plain(), secrets: InMemorySecretStore(), factory: { _ in fake })
        await session.connect()
        #expect(session.status == .failed(message: KafkaError.unreachable(host: "h1:9092").userMessage))
        #expect(throws: ClusterSessionError.notConnected) { try session.client() }
        await session.connect()
        #expect(session.status == .connected)
    }

    @Test func saslProfileReadsPasswordAtConnectAndPassesCredentials() async throws {
        let secrets = SpySecretStore()
        let profile = saslProfile()
        try await secrets.setPassword(secretPassword, for: profile.id)
        let probe = FactoryProbe(make: { FakeKafkaClient(expectedCredentials: Credentials(username: "alice", password: secretPassword)) })
        let session = makeSession(profile, probe: probe, secrets: secrets)
        let readsBefore = await secrets.callCount
        await session.connect()
        #expect(session.status == .connected)
        #expect(await secrets.callCount == readsBefore + 1)
        #expect(!String(describing: session.status).contains(secretPassword))
    }

    @Test func wrongPasswordFailsWithAuthMessageAndNeverLeaksIt() async throws {
        let secrets = InMemorySecretStore()
        let profile = saslProfile()
        try await secrets.setPassword("wrong-pw", for: profile.id)
        let probe = FactoryProbe(make: { FakeKafkaClient(expectedCredentials: Credentials(username: "alice", password: secretPassword)) })
        let session = makeSession(profile, probe: probe, secrets: secrets)
        await session.connect()
        #expect(session.status == .failed(message: KafkaError.authenticationFailed.userMessage))
        #expect(!String(describing: session.status).contains("wrong-pw"))
    }

    @Test func plaintextProfileNeverReadsSecrets() async {
        let secrets = SpySecretStore()
        let session = makeSession(plain(), secrets: secrets)
        await session.connect()
        #expect(await secrets.callCount == 0)
    }

    @Test func keychainFailureBecomesAUserMessage() async {
        let secrets = ThrowingSecrets()
        let session = makeSession(saslProfile(), secrets: secrets)
        await session.connect()
        #expect(session.status == .failed(message: SecretStoreError.keychain(status: -25293).userMessage))
    }

    @Test func productionStartsReadOnlyOthersDoNot() async throws {
        let production = makeSession(plain(.production))
        #expect(production.isReadOnly)
        let client = try await production.ensureConnected()
        await #expect(throws: KafkaError.readOnly(clusterName: "c")) { try await client.createTopic(newTopic()) }

        for environment in [ClusterConnection.Environment.local, .staging] {
            let session = makeSession(plain(environment))
            #expect(!session.isReadOnly)
            try await session.ensureConnected().createTopic(newTopic())
        }
    }

    @Test func setReadOnlyAppliesToTheLiveClient() async throws {
        let session = makeSession(plain(.production))
        let client = try await session.ensureConnected()
        await session.setReadOnly(false)
        #expect(!session.isReadOnly)
        try await client.createTopic(newTopic())
        await session.setReadOnly(true)
        await #expect(throws: KafkaError.readOnly(clusterName: "c")) { try await client.deleteTopic("spike-x") }
    }

    @Test func updatingEnvironmentResetsToIdleWithNewReadOnlyDefault() async throws {
        let probe = FactoryProbe()
        let local = plain(.local)
        let session = makeSession(local, probe: probe)
        _ = try await session.ensureConnected()
        var production = local
        production.environment = .production
        production.bootstrapServers = "other:9092"
        await session.updateConnection(production)
        #expect(session.status == .idle)
        #expect(session.isReadOnly)
        let client = try await session.ensureConnected()
        #expect(probe.callCount == 2)
        #expect(probe.connections.last?.bootstrapServers == "other:9092")
        await #expect(throws: KafkaError.readOnly(clusterName: "c")) { try await client.createTopic(newTopic()) }
    }

    @Test func updatingWithoutChangesKeepsTheConnection() async throws {
        let probe = FactoryProbe()
        let profile = plain()
        let session = makeSession(profile, probe: probe)
        _ = try await session.ensureConnected()
        await session.updateConnection(profile)
        #expect(session.status == .connected)
        #expect(probe.callCount == 1)
    }

    @Test func updatingAFailedSessionResetsIt() async {
        let session = makeSession(saslProfile(), probe: FactoryProbe(make: { FakeKafkaClient(expectedCredentials: Credentials(username: "x", password: "y")) }))
        await session.connect()
        var changed = session.connection
        changed.bootstrapServers = "new:9093"
        await session.updateConnection(changed)
        #expect(session.status == .idle)
    }

    @Test func updateKeepsAUserReadOnlyChoiceWhenEnvironmentIsUnchanged() async throws {
        let session = makeSession(plain(.production))
        await session.setReadOnly(false)
        var changed = session.connection
        changed.bootstrapServers = "other:9092"
        await session.updateConnection(changed)
        #expect(!session.isReadOnly)
    }

    // MARK: Idle expiry

    @Test func touchRestartsTheSingleIdleTimer() async throws {
        let sleeper = SleepController()
        let session = makeSession(sleeper: sleeper, idle: .seconds(42))
        await session.connect()
        await expectSettles { await sleeper.pendingCount == 1 }
        session.touch()
        await expectSettles { await sleeper.snapshot() == .init(requested: 2, pending: 1) }
        #expect(await sleeper.requested == [.seconds(42), .seconds(42)])

        await sleeper.fire(0)  // the cancelled first timer must not expire the session
        await Task.yield()
        #expect(session.status == .connected)

        await sleeper.fire(1)
        await expectSettles { session.status == .idle }
        #expect(throws: ClusterSessionError.notConnected) { _ = try session.client() }
    }

    @Test func ensureConnectedCountsAsActivity() async throws {
        let sleeper = SleepController()
        let session = makeSession(sleeper: sleeper)
        _ = try await session.ensureConnected()
        _ = try await session.ensureConnected()
        await expectSettles { await sleeper.snapshot() == .init(requested: 3, pending: 1) }
    }

    @Test func noTimerRemainsAfterDisconnect() async {
        let sleeper = SleepController()
        let session = makeSession(sleeper: sleeper)
        await session.connect()
        await expectSettles { await sleeper.pendingCount == 1 }
        await session.disconnect()
        #expect(session.status == .idle)
        await expectSettles { await sleeper.pendingCount == 0 }
    }

    @Test func touchWhenNotConnectedDoesNothing() async {
        let sleeper = SleepController()
        let session = makeSession(sleeper: sleeper)
        session.touch()
        await Task.yield()
        #expect(await sleeper.requested.isEmpty)
    }

    @Test func expiredSessionReconnectsOnDemand() async throws {
        let sleeper = SleepController()
        let probe = FactoryProbe()
        let session = makeSession(probe: probe, sleeper: sleeper)
        _ = try await session.ensureConnected()
        await expectSettles { await sleeper.snapshot() == .init(requested: 2, pending: 1) }
        await sleeper.fire(1)  // 0 was cancelled when ensureConnected touched again
        await expectSettles { session.status == .idle }
        _ = try await session.ensureConnected()
        #expect(probe.callCount == 2)
    }

    // MARK: ensureConnected

    @Test func ensureConnectedIsIdempotent() async throws {
        let probe = FactoryProbe()
        let session = makeSession(probe: probe)
        _ = try await session.ensureConnected()
        _ = try await session.ensureConnected()
        #expect(probe.callCount == 1)
    }

    @Test func tenParallelEnsureConnectedCausesOneConnect() async throws {
        let probe = FactoryProbe(make: { FakeKafkaClient(latency: .milliseconds(5)) })
        let session = makeSession(probe: probe)
        let tasks = (0..<10).map { _ in Task { try await session.ensureConnected() } }
        for task in tasks { _ = try await task.value }
        #expect(probe.callCount == 1)
        #expect(session.status == .connected)
    }

    @Test func ensureConnectedRethrowsTheConnectFailure() async {
        let session = makeSession(saslProfile(), probe: FactoryProbe(make: { FakeKafkaClient(expectedCredentials: Credentials(username: "x", password: "y")) }))
        await #expect(throws: KafkaError.authenticationFailed) { _ = try await session.ensureConnected() }
    }

    @Test func disconnectDuringConnectDiscardsTheResult() async throws {
        let probe = FactoryProbe(make: { FakeKafkaClient(latency: .milliseconds(20)) })
        let session = makeSession(probe: probe)
        let connecting = Task { @MainActor in await session.connect() }
        await expectSettles { session.status == .connecting }
        await session.disconnect()
        await connecting.value
        #expect(session.status == .idle)
        #expect(throws: ClusterSessionError.notConnected) { _ = try session.client() }
    }

    @Test func connectAfterDisconnectDuringAConnectReallyConnects() async throws {
        let probe = FactoryProbe(make: { FakeKafkaClient(latency: .milliseconds(20)) })
        let session = makeSession(probe: probe)
        let first = Task { await session.connect() }
        await expectSettles { session.status == .connecting }
        await session.disconnect()
        // The abandoned attempt may still be running; a new request must not just wait for it.
        await session.connect()
        #expect(session.status == .connected)
        #expect(probe.callCount == 2)
        await first.value
        #expect(session.status == .connected)
    }

    @Test func ensureConnectedAfterDisconnectDuringAConnectReturnsAClient() async throws {
        let probe = FactoryProbe(make: { FakeKafkaClient(latency: .milliseconds(20)) })
        let session = makeSession(probe: probe)
        let first = Task { await session.connect() }
        await expectSettles { session.status == .connecting }
        await session.disconnect()
        _ = try await session.ensureConnected()
        #expect(session.status == .connected)
        await first.value
    }

    @Test func aWaiterOfAnAbandonedConnectRetriesInsteadOfFailing() async throws {
        let probe = FactoryProbe(make: { FakeKafkaClient(latency: .milliseconds(20)) })
        let session = makeSession(probe: probe)
        let waiter = Task { try await session.ensureConnected() }
        await expectSettles { session.status == .connecting }
        await session.disconnect()
        _ = try await waiter.value
        #expect(session.status == .connected)
    }
}

@Suite("ClusterSessions") @MainActor
struct ClusterSessionsTests {
    private func makeRegistry(_ probe: FactoryProbe = FactoryProbe(), sleeper: SleepController = SleepController()) -> ClusterSessions {
        ClusterSessions(secrets: InMemorySecretStore(), factory: probe.factory, sleep: sleeper.sleep)
    }

    @Test func sameIDSameSessionDifferentIDsDifferent() async {
        let registry = makeRegistry()
        let a = plain(), b = plain()
        let first = await registry.session(for: a)
        let again = await registry.session(for: a)
        let other = await registry.session(for: b)
        #expect(first === again)
        #expect(first !== other)
        #expect(registry.openSessionCount == 2)
    }

    @Test func creatingSessionsConnectsNothing() async {
        let probe = FactoryProbe()
        let registry = makeRegistry(probe)
        let session = await registry.session(for: plain())
        #expect(session.status == .idle)
        #expect(probe.callCount == 0)
    }

    @Test func changedProfileUpdatesTheExistingSession() async throws {
        let probe = FactoryProbe()
        let registry = makeRegistry(probe)
        let profile = plain()
        let session = await registry.session(for: profile)
        _ = try await session.ensureConnected()
        var changed = profile
        changed.bootstrapServers = "new:9092"
        let same = await registry.session(for: changed)
        #expect(same === session)
        #expect(session.connection.bootstrapServers == "new:9092")
        #expect(session.status == .idle)
    }

    @Test func removeDisconnectsAndForgets() async throws {
        let registry = makeRegistry()
        let profile = plain()
        let session = await registry.session(for: profile)
        _ = try await session.ensureConnected()
        await registry.remove(id: profile.id)
        #expect(session.status == .idle)
        #expect(registry.openSessionCount == 0)
        #expect(await registry.session(for: profile) !== session)
    }

    @Test func closeKeepsTheSessionButDisconnects() async throws {
        let registry = makeRegistry()
        let profile = plain()
        let session = await registry.session(for: profile)
        _ = try await session.ensureConnected()
        await registry.close(id: profile.id)
        #expect(session.status == .idle)
        #expect(await registry.session(for: profile) === session)
    }

    @Test func closeAllDisconnectsEverySession() async throws {
        let registry = makeRegistry()
        let sessions = [await registry.session(for: plain()), await registry.session(for: plain(.production))]
        for session in sessions { _ = try await session.ensureConnected() }
        await registry.closeAll()
        #expect(sessions.allSatisfy { $0.status == .idle })
    }

    @Test func editingLocalToProductionMakesTheSessionReadOnlyAtOnce() async throws {
        let registry = makeRegistry()
        let profile = plain(.local)
        let session = await registry.session(for: profile)
        #expect(!session.isReadOnly)
        let model = ConnectionsViewModel(store: InMemoryConnectionStore(connections: [profile]), secrets: InMemorySecretStore(), observer: registry)
        await model.load()

        var edited = profile
        edited.environment = .production
        model.didSave(edited)
        await model.waitForObserverSync()
        #expect(session.isReadOnly)
        #expect(EnvironmentBannerPresentation(clusterName: edited.name, environment: session.connection.environment, isReadOnly: session.isReadOnly).toggleNeedsConfirmation)
    }

    @Test func editingLocalToProductionWhileConnectedBlocksWritesOnTheNextClient() async throws {
        let registry = makeRegistry()
        let profile = plain(.local)
        let session = await registry.session(for: profile)
        _ = try await session.ensureConnected()
        let model = ConnectionsViewModel(store: InMemoryConnectionStore(connections: [profile]), secrets: InMemorySecretStore(), observer: registry)
        await model.load()

        var edited = profile
        edited.environment = .production
        model.didSave(edited)
        await model.waitForObserverSync()
        #expect(session.isReadOnly)
        let client = try await session.ensureConnected()
        await #expect(throws: KafkaError.self) { try await client.deleteTopic("spike-x") }
    }

    @Test func connectionsViewModelNotifiesTheRegistry() async throws {
        let registry = makeRegistry()
        let profile = plain()
        let session = await registry.session(for: profile)
        _ = try await session.ensureConnected()

        let store = InMemoryConnectionStore(connections: [profile])
        let model = ConnectionsViewModel(store: store, secrets: InMemorySecretStore(), observer: registry)
        await model.load()
        var edited = profile
        edited.name = "renamed"
        model.didSave(edited)
        await model.waitForObserverSync()
        #expect(session.connection.name == "renamed")
        #expect(session.status == .idle)

        _ = try await session.ensureConnected()
        await model.delete(id: profile.id)
        #expect(session.status == .idle)
        #expect(registry.openSessionCount == 0)
    }
}

// MARK: Helpers

private struct ThrowingSecrets: SecretStore {
    func password(for id: UUID) async throws -> String? { throw SecretStoreError.keychain(status: -25293) }
    func setPassword(_ password: String, for id: UUID) async throws {}
    func deletePassword(for id: UUID) async throws {}
}
