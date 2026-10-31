import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private func seed(_ name: String) -> FakeKafkaClient.SeedTopic {
    FakeKafkaClient.SeedTopic(detail: TopicDetail(
        name: name, partitions: [PartitionInfo(id: 0, leader: 1, replicas: [1], isr: [1], startOffset: 0, endOffset: 0)]))
}

@MainActor
private struct TopicRig {
    let fake: FakeKafkaClient
    let session: ClusterSession
    let list: TopicListViewModel

    init(_ names: [String] = ["orders"], environment: ClusterConnection.Environment = .local) {
        let fake = FakeKafkaClient(topics: names.map(seed))
        self.fake = fake
        session = ClusterSession(
            connection: ClusterConnection(name: "dev-cluster", bootstrapServers: "h:9092", environment: environment),
            secrets: InMemorySecretStore(), factory: { _ in fake })
        list = TopicListViewModel(session: session)
    }
}

@Suite("CreateTopicFormViewModel") @MainActor
struct CreateTopicFormViewModelTests {
    @Test func defaultsAreOnePartitionOneReplicaAndNoOverrides() {
        let form = TopicRig().list.makeCreateForm()
        #expect(form.name.isEmpty)
        #expect(form.partitions == 1 && form.replicationFactor == 1)
        #expect(form.retentionMs.isEmpty && form.cleanupPolicy == nil)
        #expect(form.submitState == .idle)
        #expect(form.newTopic.config.isEmpty)
    }

    @Test func issuesShowOnlyAfterAFieldIsTouched() {
        let form = TopicRig().list.makeCreateForm()
        #expect(!form.isValid)
        #expect(form.visibleIssues.isEmpty)
        form.touch(.name)
        #expect(form.issues(for: .name).first?.message == TopicNameProblem.empty.message)
        form.name = "bad name"
        #expect(form.issues(for: .name).first?.message == TopicNameProblem.invalidCharacters([" "]).message)
        form.name = "good"
        #expect(form.isValid)
        #expect(form.visibleIssues.isEmpty)
    }

    @Test func numbersAndRetentionAreValidated() {
        let form = TopicRig().list.makeCreateForm()
        form.name = "t"
        form.partitions = 0
        form.replicationFactor = 0
        form.retentionMs = "abc"
        #expect(form.issues.filter(\.isError).map(\.field).sorted { "\($0)" < "\($1)" } == [.partitions, .replicationFactor, .retention])
        form.retentionMs = "-1"
        form.partitions = 3
        form.replicationFactor = 2
        #expect(form.isValid)
        form.retentionMs = "-2"
        #expect(!form.isValid)
    }

    @Test func existingTopicIsRejectedBeforeAnyCall() async {
        let rig = TopicRig(["orders"])
        await rig.list.loadIfNeeded()
        let form = rig.list.makeCreateForm()
        form.name = "orders"
        #expect(!form.isValid)
        #expect(form.issues.first?.message == KafkaError.topicAlreadyExists("orders").userMessage)
    }

    @Test func mixedDotAndUnderscoreIsOnlyAWarning() {
        let form = TopicRig().list.makeCreateForm()
        form.name = "a.b_c"
        #expect(form.isValid)
        #expect(form.issues.contains { !$0.isError && $0.field == .name })
    }

    @Test func createBuildsTheSpecWithOptionalConfig() async throws {
        let rig = TopicRig()
        let form = rig.list.makeCreateForm()
        form.name = "fresh"
        form.partitions = 3
        form.replicationFactor = 2
        form.retentionMs = " 60000 "
        form.cleanupPolicy = .compactAndDelete
        await form.create()
        #expect(form.submitState == .created(name: "fresh"))
        let detail = try await rig.fake.describeTopic("fresh")
        #expect(detail.partitions.count == 3)
        #expect(detail.config == ["retention.ms": "60000", "cleanup.policy": "compact,delete"])
    }

    @Test func successRefreshesTheListAndSelectsTheNewTopic() async {
        let rig = TopicRig(["orders"])
        await rig.list.loadIfNeeded()
        let form = rig.list.makeCreateForm()
        form.name = "fresh"
        await form.create()
        #expect(rig.list.state.value?.map(\.name) == ["fresh", "orders"])
        #expect(rig.list.selection == "fresh")
    }

    @Test func brokerFailureShowsTheUserMessageAndAllowsAnotherTry() async {
        let rig = TopicRig()
        await rig.fake.failing(.createTopic, with: .authorizationFailed(resource: "cluster"))
        let form = rig.list.makeCreateForm()
        form.name = "fresh"
        await form.create()
        #expect(form.submitState == .failed(message: KafkaError.authorizationFailed(resource: "cluster").userMessage))
        #expect(rig.list.selection == nil)
        await rig.fake.failing(.createTopic, with: nil)
        await form.create()
        #expect(form.submitState == .created(name: "fresh"))
    }

    @Test func replicationFactorAboveBrokerCountGivesATranslatedMessage() async {
        let rig = TopicRig()
        let form = rig.list.makeCreateForm()
        form.name = "fresh"
        form.replicationFactor = 5
        await form.create()
        #expect(form.submitState == .failed(message: KafkaError.clusterError(code: KafkaError.invalidReplicationFactorCode, message: "").userMessage))
    }

    @Test func secondSubmitWhileCreatingDoesNothing() async {
        let rig = TopicRig()
        _ = try? await rig.session.ensureConnected()
        await rig.fake.setLatency(.milliseconds(300))
        let form = rig.list.makeCreateForm()
        form.name = "fresh"
        form.submit()
        await expectSettles { form.isCreating }
        #expect(!form.canSubmit)
        form.submit()
        await form.create()
        for _ in 0..<100 where form.submitState != .created(name: "fresh") { try? await Task.sleep(for: .milliseconds(20)) }
        #expect(form.submitState == .created(name: "fresh"))
        let names = (try? await rig.fake.listTopics().map(\.name)) ?? []
        #expect(names.filter { $0 == "fresh" }.count == 1)
    }

    @Test func readOnlyClusterDisablesCreateAndNeverCallsTheBroker() async {
        let rig = TopicRig(environment: .production)
        let form = rig.list.makeCreateForm()
        form.name = "fresh"
        #expect(!form.canSubmit)
        #expect(form.gate.disabledReason == "This cluster is read-only")
        await form.create()
        #expect(form.submitState == .failed(message: KafkaError.readOnly(clusterName: "dev-cluster").userMessage))
        #expect(rig.session.status == .idle)
    }

    @Test func turningReadOnlyOffEnablesCreateOnProduction() async {
        let rig = TopicRig(environment: .production)
        let form = rig.list.makeCreateForm()
        form.name = "fresh"
        await rig.session.setReadOnly(false)
        #expect(form.canSubmit)
        await form.create()
        #expect(form.submitState == .created(name: "fresh"))
    }

    @Test func readOnlyReachedTheCoreGuardToo() async throws {
        // Even if the UI offered it, the guarded client refuses.
        let rig = TopicRig(environment: .production)
        let client = try await rig.session.ensureConnected()
        await #expect(throws: KafkaError.readOnly(clusterName: "dev-cluster")) {
            try await client.createTopic(NewTopic(name: "x", partitions: 1, replicationFactor: 1))
        }
        await #expect(throws: KafkaError.readOnly(clusterName: "dev-cluster")) { try await client.deleteTopic("orders") }
    }
}

@Suite("Delete topic flow") @MainActor
struct DeleteTopicFlowTests {
    @Test func requestBuildsAConfirmationNamingClusterEnvironmentAndTopic() {
        let rig = TopicRig(["orders"], environment: .staging)
        rig.list.requestDelete(of: "orders")
        let confirmation = rig.list.pendingDelete
        #expect(confirmation?.objectName == "orders")
        #expect(confirmation?.clusterName == "dev-cluster")
        #expect(confirmation?.environment == .staging)
        #expect(confirmation?.consequences.contains("dev-cluster") == true)
        #expect(confirmation?.isSatisfied(by: "orders") == true)
        #expect(confirmation?.isSatisfied(by: "Orders") == false)
    }

    @Test func readOnlyClusterOffersNoConfirmation() {
        let rig = TopicRig(environment: .production)
        rig.list.requestDelete(of: "orders")
        #expect(rig.list.pendingDelete == nil)
        #expect(!rig.list.writeGate.allowsWrites)
    }

    @Test func successDeletesThenRefreshesAndClearsTheSelection() async {
        let rig = TopicRig(["orders", "payments"])
        await rig.list.loadIfNeeded()
        rig.list.selection = "payments"
        await rig.list.delete("payments")
        #expect(rig.list.state.value?.map(\.name) == ["orders"])
        #expect(rig.list.selection == nil)
        #expect(rig.list.deleteError == nil)
        #expect(!rig.list.isDeleting)
    }

    @Test func failureKeepsTheListAndTheSelectionAndShowsTheMessage() async {
        let rig = TopicRig(["orders", "payments"])
        await rig.list.loadIfNeeded()
        rig.list.selection = "payments"
        await rig.fake.failing(.deleteTopic, with: .authorizationFailed(resource: "topic payments"))
        await rig.list.delete("payments")
        #expect(rig.list.state.value?.map(\.name) == ["orders", "payments"])
        #expect(rig.list.selection == "payments")
        #expect(rig.list.deleteError == KafkaError.authorizationFailed(resource: "topic payments").userMessage)
        rig.list.dismissDeleteError()
        #expect(rig.list.deleteError == nil)
    }

    @Test func nothingIsRemovedBeforeTheBrokerAnswers() async {
        let rig = TopicRig(["orders"])
        await rig.list.loadIfNeeded()
        await rig.fake.setLatency(.milliseconds(300))
        let task = Task { await rig.list.delete("orders") }
        await expectSettles { rig.list.isDeleting }
        #expect(rig.list.state.value?.map(\.name) == ["orders"])
        await task.value
        #expect(rig.list.state.value?.isEmpty == true)
    }

    @Test func readOnlyDeleteGivesTheFriendlyMessageAndNoBrokerCall() async {
        let rig = TopicRig(["orders"], environment: .production)
        await rig.list.delete("orders")
        #expect(rig.list.deleteError == KafkaError.readOnly(clusterName: "dev-cluster").userMessage)
        #expect(rig.session.status == .idle)
    }

    @Test func confirmPendingDeleteRunsTheDeleteOnce() async {
        let rig = TopicRig(["orders"])
        await rig.list.loadIfNeeded()
        rig.list.requestDelete(of: "orders")
        rig.list.confirmPendingDelete()
        rig.list.confirmPendingDelete()
        for _ in 0..<100 where rig.list.state.value?.isEmpty != true { try? await Task.sleep(for: .milliseconds(20)) }
        #expect(rig.list.deleteError == nil)
    }
}
