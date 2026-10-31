import Testing
import Foundation
@testable import KafkaManagerCore

private func lag(_ partition: Int, committed: Int64?, start: Int64 = 0, end: Int64) -> PartitionLag {
    lag("orders", partition, committed: committed, start: start, end: end)
}

private func lag(_ topic: String, _ partition: Int, committed: Int64?, start: Int64 = 0, end: Int64) -> PartitionLag {
    PartitionLag(topic: topic, partition: partition, committedOffset: committed, startOffset: start, endOffset: end)
}

private func rows(_ lags: [PartitionLag], _ target: OffsetResetTarget, scope: OffsetResetScope = .wholeGroup,
                  times: [PartitionKey: Int64] = [:]) -> [OffsetResetPreviewRow] {
    OffsetResetPlanner.rows(lags: lags, target: target, scope: scope, timestampOffsets: times)
}

@Suite("OffsetResetPlanner")
struct OffsetResetPlannerTests {
    @Test func latestMovesToTheLogEndAndClearsTheLag() {
        let result = rows([lag(0, committed: 10, end: 100), lag(1, committed: 100, end: 100)], .latest)
        #expect(result.map(\.newOffset) == [100, 100])
        #expect(result.map(\.lagBefore) == [90, 0])
        #expect(result.map(\.lagAfter) == [0, 0])
        #expect(result.map(\.willChange) == [true, false])
    }

    @Test func earliestMovesToTheLogStart() {
        let result = rows([lag(0, committed: 90, start: 20, end: 100)], .earliest)
        #expect(result[0].newOffset == 20)
        #expect(result[0].lagBefore == 10)
        #expect(result[0].lagAfter == 80)
    }

    @Test func timestampUsesTheLookupAndFallsBackToTheEnd() {
        let lags = [lag(0, committed: 5, end: 100), lag(1, committed: 5, end: 100), lag(2, committed: 5, end: 100)]
        let times = [PartitionKey(topic: "orders", partition: 0): Int64(40), PartitionKey(topic: "orders", partition: 1): Int64(-1)]
        let result = rows(lags, .timestamp(Date()), times: times)
        // Found, "no message at or after" (-1), and not answered at all: the last two go to the end.
        #expect(result.map(\.newOffset) == [40, 100, 100])
        #expect(result.allSatisfy { !$0.isClamped })
    }

    @Test func offsetIsClampedIntoTheRetainedRange() {
        let lags = [lag(0, committed: 50, start: 10, end: 100)]
        #expect(rows(lags, .offset(5))[0].newOffset == 10)
        #expect(rows(lags, .offset(5))[0].isClamped)
        #expect(rows(lags, .offset(500))[0].newOffset == 100)
        #expect(rows(lags, .offset(500))[0].isClamped)
        let inside = rows(lags, .offset(60))[0]
        #expect(inside.newOffset == 60 && !inside.isClamped && inside.lagAfter == 40)
    }

    @Test func aPartitionWithoutACommitCountsItsWholeRangeAsLagAndGetsOne() {
        let result = rows([lag(0, committed: nil, start: 10, end: 60)], .latest)
        #expect(result[0].lagBefore == 50 && result[0].lagAfter == 0)
        #expect(result[0].willChange)
        // Even rewinding to the start creates a commit.
        #expect(rows([lag(0, committed: nil, start: 10, end: 60)], .earliest)[0].willChange)
    }

    @Test func committedAheadOfTheEndIsPulledBack() {
        let result = rows([lag(0, committed: 150, end: 100)], .latest)
        #expect(result[0].newOffset == 100 && result[0].willChange)
        #expect(result[0].lagBefore == 0 && result[0].lagAfter == 0)
    }

    @Test func scopeFiltersTopicsAndPartitions() {
        let lags = [lag("a", 0, committed: 1, end: 9), lag("a", 1, committed: 1, end: 9), lag("b", 0, committed: 1, end: 9)]
        #expect(rows(lags, .latest, scope: .topics(["a"])).map(\.id) == ["a#0", "a#1"])
        #expect(rows(lags, .latest, scope: .partitions(topic: "a", [1])).map(\.id) == ["a#1"])
        #expect(rows(lags, .latest, scope: .topics([])).isEmpty)
        #expect(rows(lags, .latest).count == 3)
    }

    @Test func rowsAreOrderedByTopicThenPartition() {
        let lags = [lag("b", 1, committed: 1, end: 9), lag("a", 2, committed: 1, end: 9), lag("a", 0, committed: 1, end: 9)]
        #expect(rows(lags, .latest).map(\.id) == ["a#0", "a#2", "b#1"])
    }

    @Test func totalsSumLagAndSaturateInsteadOfOverflowing() {
        let big = Int64.max - 10
        let preview = OffsetResetPlanner.preview(
            detail: GroupDetail(id: "g", state: "Empty", members: [], lags: [lag(0, committed: 0, end: big), lag(1, committed: 0, end: big)]),
            target: .latest)
        #expect(preview.totalLagBefore == Int64.max)
        #expect(preview.totalLagAfter == 0)
        let small = OffsetResetPlanner.preview(
            detail: GroupDetail(id: "g", state: "Empty", members: [], lags: [lag(0, committed: 0, end: 7), lag(1, committed: 5, end: 9)]),
            target: .latest)
        #expect(small.totalLagBefore == 11 && small.totalLagAfter == 0)
        #expect(small.groupState == .empty)
    }

    @Test func aGroupWithNothingToResetHasAnEmptyPlan() {
        let preview = OffsetResetPlanner.preview(detail: GroupDetail(id: "g", state: "Empty", members: [], lags: []), target: .latest)
        #expect(preview.rows.isEmpty && preview.plan.isEmpty)
        let settled = OffsetResetPlanner.preview(
            detail: GroupDetail(id: "g", state: "Empty", members: [], lags: [lag(0, committed: 9, end: 9)]), target: .latest)
        #expect(settled.changedRows.isEmpty && settled.plan.isEmpty)
    }

    @Test func thePlanHoldsOnlyWhatChanges() {
        let preview = OffsetResetPlanner.preview(
            detail: GroupDetail(id: "g", state: "Empty", members: [], lags: [lag(0, committed: 1, end: 9), lag(1, committed: 9, end: 9)]),
            target: .latest)
        #expect(preview.plan.assignments == [OffsetResetAssignment(topic: "orders", partition: 0, offset: 9)])
        #expect(preview.limited(to: .partitions(topic: "orders", [1])).plan.isEmpty)
    }

    @Test func onlyEmptyAndDeadGroupsMayBeChanged() {
        for state: GroupState in [.empty, .dead] { #expect(OffsetResetRule.refusal(for: state) == nil) }
        for state: GroupState in [.stable, .preparingRebalance, .completingRebalance, .other("x")] {
            #expect(OffsetResetRule.refusal(for: state) == OffsetResetRule.activeMembersMessage)
        }
        #expect(OffsetResetRule.activeMembersMessage == "Stop the group's consumers first; offsets can only be changed while the group has no active members.")
    }
}

@Suite("Fake offset reset")
struct FakeOffsetResetTests {
    private let cluster = ClusterConnection(name: "c", bootstrapServers: "h:9092")

    private func fake() async throws -> FakeKafkaClient {
        let member = GroupMemberInfo(id: "m", clientId: "c", host: "h")
        let fake = FakeKafkaClient(groups: [
            GroupDetail(id: "idle", state: "Empty", members: [], lags: [lag(0, committed: 10, end: 100), lag(1, committed: nil, end: 40)]),
            GroupDetail(id: "busy", state: "Stable", members: [member], lags: [lag(0, committed: 10, end: 100)]),
        ])
        try await fake.connect(cluster, credentials: nil)
        return fake
    }

    @Test func previewReadsAndChangesNothing() async throws {
        let fake = try await fake()
        let preview = try await fake.previewOffsetReset(group: "idle", target: .latest, scope: .wholeGroup)
        #expect(preview.rows.map(\.newOffset) == [100, 40])
        #expect(try await fake.describeGroup("idle").lags.first?.committedOffset == 10)
    }

    @Test func applyCommitsTheNewOffsetsAndTheLagDisappears() async throws {
        let fake = try await fake()
        let preview = try await fake.previewOffsetReset(group: "idle", target: .latest, scope: .wholeGroup)
        let result = try await fake.resetOffsets(group: "idle", plan: preview.plan)
        #expect(result.applied == 2 && result.isComplete)
        let lags = try await fake.describeGroup("idle").lags
        #expect(lags.map(\.committedOffset) == [100, 40])
        #expect(lags.allSatisfy { $0.lag == 0 })
    }

    @Test func aGroupWithMembersIsRefusedAndNothingMoves() async throws {
        let fake = try await fake()
        let plan = OffsetResetPlan(assignments: [OffsetResetAssignment(topic: "orders", partition: 0, offset: 100)])
        do { _ = try await fake.resetOffsets(group: "busy", plan: plan); Issue.record("expected a refusal") } catch let error as KafkaError {
            #expect(error.isNonEmptyGroup)
            #expect(error.userMessage == OffsetResetRule.activeMembersMessage)
        }
        #expect(try await fake.describeGroup("busy").lags.first?.committedOffset == 10)
    }

    @Test func unknownGroupIsTheSameErrorAsDescribeGroup() async throws {
        let fake = try await fake()
        do { _ = try await fake.resetOffsets(group: "nope", plan: OffsetResetPlan(assignments: [])); Issue.record("expected unknown group") } catch let error as KafkaError {
            #expect(error.isUnknownGroup)
        }
        do { _ = try await fake.previewOffsetReset(group: "nope", target: .latest, scope: .wholeGroup); Issue.record("expected unknown group") } catch let error as KafkaError {
            #expect(error.isUnknownGroup)
        }
    }

    @Test func aPartitionTheGroupDoesNotHaveIsAPartialFailure() async throws {
        let fake = try await fake()
        let plan = OffsetResetPlan(assignments: [
            OffsetResetAssignment(topic: "orders", partition: 0, offset: 100), OffsetResetAssignment(topic: "ghost", partition: 3, offset: 1),
        ])
        let result = try await fake.resetOffsets(group: "idle", plan: plan)
        #expect(result.applied == 1)
        #expect(result.failures.map(\.topic) == ["ghost"])
    }

    @Test func failureInjectionWorksForBothCalls() async throws {
        let fake = try await fake()
        await fake.failing(.previewOffsetReset, with: .timeout)
        await #expect(throws: KafkaError.timeout) { _ = try await fake.previewOffsetReset(group: "idle", target: .latest, scope: .wholeGroup) }
        await fake.failing(.resetOffsets, with: .authorizationFailed(resource: "group idle"))
        await #expect(throws: KafkaError.authorizationFailed(resource: "group idle")) { _ = try await fake.resetOffsets(group: "idle", plan: OffsetResetPlan(assignments: [])) }
    }

    @Test func notConnectedFailsLikeTheOtherCalls() async {
        let fake = FakeKafkaClient(groups: [GroupDetail(id: "idle", state: "Empty", members: [], lags: [])])
        await #expect(throws: KafkaError.self) { _ = try await fake.previewOffsetReset(group: "idle", target: .latest, scope: .wholeGroup) }
    }
}
