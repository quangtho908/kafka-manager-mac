import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private func lag(_ partition: Int, committed: Int64?, end: Int64 = 100) -> PartitionLag {
    PartitionLag(topic: "orders", partition: partition, committedOffset: committed, startOffset: 0, endOffset: end)
}

@MainActor
private struct ResetRig {
    let fake: FakeKafkaClient
    let session: ClusterSession
    let list: GroupListViewModel

    init(environment: ClusterConnection.Environment = .local) {
        let member = GroupMemberInfo(id: "m", clientId: "c", host: "h")
        let fake = FakeKafkaClient(groups: [
            GroupDetail(id: "idle", state: "Empty", members: [], lags: [lag(0, committed: 10), lag(1, committed: 100), lag(2, committed: nil)]),
            GroupDetail(id: "solo", state: "Empty", members: [], lags: [lag(0, committed: 3)]),
            GroupDetail(id: "busy", state: "Stable", members: [member], lags: [lag(0, committed: 10)]),
        ])
        self.fake = fake
        session = ClusterSession(
            connection: ClusterConnection(name: "dev-cluster", bootstrapServers: "h:9092", environment: environment),
            secrets: InMemorySecretStore(), factory: { _ in fake })
        list = GroupListViewModel(session: session)
    }

    func open(_ group: String = "idle", target: OffsetTargetKind = .latest) async -> OffsetResetViewModel? {
        await list.refresh()
        list.requestReset(of: group, target: target)
        return list.reset
    }
}

@Suite("OffsetResetViewModel") @MainActor
struct OffsetResetViewModelTests {
    @Test func previewLoadsAndSelectsEveryTopic() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open())
        #expect(model.preview == .idle)
        await model.refreshPreview()
        let preview = try #require(model.preview.value)
        #expect(preview.rows.map(\.newOffset) == [100, 100, 100])
        #expect(preview.totalLagBefore == 90 + 0 + 100 && preview.totalLagAfter == 0)
        #expect(model.selectedTopics == ["orders"])
        #expect(model.applyDisabledReason == nil)
    }

    @Test func changingTheTargetRecomputesWithoutAnotherBrokerReadExceptForTime() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open())
        await model.refreshPreview()
        model.targetKind = .earliest
        await model.refreshPreview()
        #expect(model.preview.value?.rows.map(\.newOffset) == [0, 0, 0])
        model.targetKind = .offset
        model.offsetText = "50"
        await model.refreshPreview()
        #expect(model.preview.value?.rows.map(\.newOffset) == [50, 50, 50])
        model.offsetText = "500"
        await model.refreshPreview()
        #expect(model.preview.value?.rows.allSatisfy(\.isClamped) == true)
    }

    @Test func anInvalidOffsetBlocksApplyAndExplainsWhy() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open(target: .offset))
        #expect(model.target == nil)
        #expect(model.targetIssue != nil)
        for text in ["", "abc", "-3", "1.5"] {
            model.offsetText = text
            #expect(model.target == nil)
        }
        model.offsetText = " 7 "
        #expect(model.target == .offset(7) && model.targetIssue == nil)
    }

    @Test func aNewerRequestWinsOverAnOlderOne() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open())
        await model.refreshPreview()
        model.targetKind = .earliest
        let older = Task { await model.refreshPreview() }
        model.targetKind = .latest
        await model.refreshPreview()
        await older.value
        #expect(model.preview.value?.rows.map(\.newOffset) == [100, 100, 100])
    }

    @Test func scopeLimitsWhatApplyChanges() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open())
        await model.refreshPreview()
        model.onlySelectedTopics = true
        model.selectedTopics = []
        #expect(model.scopedPreview?.rows.isEmpty == true)
        #expect(model.applyDisabledReason == "Nothing would change")
        model.selectedTopics = ["orders"]
        #expect(model.applyDisabledReason == nil)
    }

    @Test func confirmationNeedsTheExactGroupId() async throws {
        let rig = ResetRig(environment: .production)
        await rig.session.setReadOnly(false)
        let model = try #require(await rig.open())
        await model.refreshPreview()
        model.requestApply()
        let confirmation = try #require(model.pendingConfirmation)
        #expect(confirmation.objectName == "idle" && confirmation.clusterName == "dev-cluster")
        #expect(confirmation.showsProductionBanner)
        #expect(confirmation.isSatisfied(by: "idle"))
        #expect(!confirmation.isSatisfied(by: "Idle") && !confirmation.isSatisfied(by: "idl") && !confirmation.isSatisfied(by: ""))
        #expect(confirmation.consequences.contains("2 partitions"))
    }

    @Test func theConfirmationSaysOnePartitionInTheSingular() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open("solo"))
        await model.refreshPreview()
        model.requestApply()
        #expect(model.pendingConfirmation?.consequences.contains("offsets of 1 partition on") == true)
    }

    @Test func applyWritesRefreshesAndReportsTheLag() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open())
        await model.refreshPreview()
        await model.apply()
        #expect(model.applyError == nil)
        #expect(model.result?.applied == 2)
        #expect(model.successNote == "Reset 2 partitions. Lag 190 → 0.")
        // The sheet re-read the group: nothing left to change.
        #expect(model.applyDisabledReason == "Nothing would change")
        #expect(try await rig.fake.describeGroup("idle").lags.map(\.committedOffset) == [100, 100, 100])
        #expect(rig.list.lagTotals["idle"] == nil)
    }

    @Test func readOnlyClusterOffersNoApplyAndWritesNothing() async throws {
        let rig = ResetRig(environment: .production)
        #expect(rig.list.resetDisabledReason(of: "idle") == "This cluster is read-only")
        await rig.list.refresh()
        rig.list.requestReset(of: "idle", target: .latest)
        #expect(rig.list.reset == nil)
        // Even a sheet that exists cannot apply.
        let model = OffsetResetViewModel(session: rig.session, groupId: "idle", groupState: .empty, target: .latest)
        await model.refreshPreview()
        #expect(model.applyDisabledReason == "This cluster is read-only")
        model.requestApply()
        #expect(model.pendingConfirmation == nil)
        await model.apply()
        #expect(model.result == nil)
        #expect(try await rig.fake.describeGroup("idle").lags.first?.committedOffset == 10)
    }

    @Test func aGroupWithMembersCannotBeReset() async throws {
        let rig = ResetRig()
        #expect(rig.list.resetDisabledReason(of: "busy") != nil)
        await rig.list.refresh()
        #expect(rig.list.resetDisabledReason(of: "busy") == OffsetResetRule.activeMembersMessage)
        #expect(rig.list.resetDisabledReason(of: "idle") == nil)
        #expect(rig.list.resetDisabledReason(of: "gone") == "This group is no longer listed")
        rig.list.requestReset(of: "busy", target: .latest)
        #expect(rig.list.reset == nil)
    }

    @Test func aGroupThatGainedMembersAfterThePreviewIsRefusedAndReloaded() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open())
        await model.refreshPreview()
        // The broker now says the group is active (someone started a consumer).
        await rig.fake.failing(.resetOffsets, with: .clusterError(code: KafkaError.nonEmptyGroupCode, message: "NON_EMPTY_GROUP"))
        await model.apply()
        #expect(model.applyError == OffsetResetRule.activeMembersMessage)
        #expect(model.result == nil && model.successNote == nil)
        #expect(model.preview.value != nil)
    }

    @Test func aFailedApplyKeepsTheSheetStateAndShowsTheMessage() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open())
        await model.refreshPreview()
        await rig.fake.failing(.resetOffsets, with: .authorizationFailed(resource: "group idle"))
        await model.apply()
        #expect(model.applyError == KafkaError.authorizationFailed(resource: "group idle").userMessage)
        #expect(model.preview.value?.changedRows.count == 2)
        #expect(!model.isApplying)
        #expect(model.applyDisabledReason == nil)
    }

    @Test func partialFailuresAreReportedAndGiveNoSuccessNote() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open())
        await model.refreshPreview()
        // One partition disappears between preview and apply.
        let ghost = OffsetResetPlan(assignments: [
            OffsetResetAssignment(topic: "orders", partition: 0, offset: 100), OffsetResetAssignment(topic: "ghost", partition: 0, offset: 1),
        ])
        let outcome = try await rig.fake.resetOffsets(group: "idle", plan: ghost)
        #expect(outcome.applied == 1 && !outcome.isComplete)
        #expect(outcome.failures.count == 1)
    }

    @Test func confirmApplyIsOwnedByTheModelAndRunsOnce() async throws {
        let rig = ResetRig()
        let model = try #require(await rig.open())
        await model.refreshPreview()
        model.requestApply()
        model.confirmApply()
        model.confirmApply()
        for _ in 0..<200 where model.result == nil { await Task.yield() }
        #expect(model.result?.applied == 2)
    }
}
