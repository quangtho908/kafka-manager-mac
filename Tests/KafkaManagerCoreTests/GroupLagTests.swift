import Testing
import Foundation
@testable import KafkaManagerCore

private func mk(_ topic: String = "t", _ partition: Int = 0, committed: Int64? = nil, start: Int64 = 0, end: Int64) -> PartitionLag {
    PartitionLag(topic: topic, partition: partition, committedOffset: committed, startOffset: start, endOffset: end)
}

@Suite("GroupState")
struct GroupStateTests {
    @Test func parsesKafkaNamesIgnoringCaseAndSpaces() {
        #expect(GroupState(kafkaName: "Stable") == .stable)
        #expect(GroupState(kafkaName: " stable ") == .stable)
        #expect(GroupState(kafkaName: "PreparingRebalance") == .preparingRebalance)
        #expect(GroupState(kafkaName: "COMPLETINGREBALANCE") == .completingRebalance)
        #expect(GroupState(kafkaName: "Empty") == .empty)
        #expect(GroupState(kafkaName: "Dead") == .dead)
    }

    @Test func unknownNamesAreKeptAsReceived() {
        #expect(GroupState(kafkaName: "Assigning") == .other("Assigning"))
        #expect(GroupState(kafkaName: "") == .other(""))
        #expect(GroupSummary(id: "g", state: "Reconciling", memberCount: 0).groupState == .other("Reconciling"))
    }

    @Test func rebalancingCoversBothPhases() {
        #expect(GroupState.preparingRebalance.isRebalancing && GroupState.completingRebalance.isRebalancing)
        #expect(!GroupState.stable.isRebalancing && !GroupState.other("x").isRebalancing)
    }
}

@Suite("Lag logic")
struct GroupLagTests {
    @Test func totalSumsPartitionLagAndIsEmptyZero() {
        #expect(GroupLag.total([]) == 0)
        #expect(GroupLag.total([mk(committed: 5, end: 10), mk(committed: 0, end: 7)]) == 12)
    }

    @Test func noCommitCountsTheRetainedRangeNotZero() {
        let uncommitted = mk(start: 100, end: 160)
        #expect(!uncommitted.hasCommittedOffset)
        #expect(uncommitted.lag == 60)
        #expect(GroupLag.commitText(uncommitted) == "no offset")
        #expect(GroupLag.commitText(mk(committed: 42, end: 50)) == "42")
    }

    @Test func committedAheadOfEndClampsToZeroAndWarns() {
        let ahead = mk(committed: 120, end: 100)
        #expect(ahead.lag == 0 && ahead.isCommittedAheadOfEnd)
        let rows = GroupLag.byTopic([ahead, mk("t", 1, committed: 90, end: 100)])
        #expect(rows[0].hasCommittedAheadOfEnd && rows[0].hasWarning)
        #expect(rows[0].totalLag == 10)
    }

    @Test func groupsByTopicOrderedWithTotals() {
        let rows = GroupLag.byTopic([
            mk("b", 1, committed: 0, end: 5), mk("a", 2, committed: 3, end: 4), mk("b", 0, end: 10), mk("a", 0, committed: 0, end: 2),
        ])
        #expect(rows.map(\.topic) == ["a", "b"])
        #expect(rows[0].partitions.map(\.partition) == [0, 2])
        #expect(rows[0].totalLag == 3 && rows[1].totalLag == 15)
        #expect(!rows[0].hasUncommittedPartitions && rows[1].hasUncommittedPartitions)
    }

    @Test func totalSaturatesInsteadOfOverflowing() {
        #expect(GroupLag.total([mk(committed: 0, end: .max), mk(committed: 0, end: .max)]) == .max)
    }

    @Test func severityThresholds() {
        let t = LagThresholds.standard
        #expect(t.severity(of: 0) == .none && t.severity(of: -5) == .none)
        #expect(t.severity(of: 1) == .low && t.severity(of: 1_000) == .low)
        #expect(t.severity(of: 1_001) == .medium && t.severity(of: 10_000) == .medium)
        #expect(t.severity(of: 10_001) == .high)
        #expect(LagSeverity.none < LagSeverity.low && LagSeverity.low < LagSeverity.medium && LagSeverity.medium < LagSeverity.high)
    }

    @Test func customThresholdsAreOrdered() {
        let t = LagThresholds(medium: 50, high: 10)
        #expect(t.high == 50)
        #expect(t.severity(of: 51) == .high)
        #expect(LagThresholds(medium: -1, high: -1).severity(of: 1) == .high)
    }

    @Test func worstPartitionDrivesGroupSeverity() {
        let lags = [mk(committed: 0, end: 5), mk("t", 1, committed: 0, end: 20_000)]
        #expect(GroupLag.severity(of: lags) == .high)
        #expect(GroupLag.severity(of: []) == .none)
    }

    @Test func formatting() {
        #expect(GroupLag.shortText(0) == "0" && GroupLag.shortText(999) == "999")
        #expect(GroupLag.shortText(1_000) == "1K" && GroupLag.shortText(1_234) == "1.2K")
        #expect(GroupLag.shortText(1_500_000) == "1.5M" && GroupLag.shortText(2_000_000_000) == "2B")
        #expect(GroupLag.shortText(-4) == "0")
        #expect(GroupLag.exactText(1_234_567).contains("234"))
        #expect(GroupLag.exactText(-1) == "0")
    }
}

@Suite("Sample groups")
struct SampleGroupTests {
    @Test func hasFortyGroupsWithEveryState() async throws {
        let fake = FakeKafkaClient.sampleData(topicCount: 10)
        try await fake.connect(ClusterConnection(name: "f", bootstrapServers: "h:9092"), credentials: nil)
        let groups = try await fake.listGroups()
        #expect(groups.count == FakeKafkaClient.sampleGroupCount)
        let states = Set(groups.map(\.groupState))
        #expect(states.isSuperset(of: [.stable, .preparingRebalance, .completingRebalance, .empty, .dead]))
        #expect(Set(groups.map(\.id)).count == groups.count)
        #expect(try await fake.listGroups() == groups)
    }

    @Test func everyGroupDescribesWithLag() async throws {
        let fake = FakeKafkaClient.sampleData(topicCount: 10)
        try await fake.connect(ClusterConnection(name: "f", bootstrapServers: "h:9092"), credentials: nil)
        for summary in try await fake.listGroups() {
            let detail = try await fake.describeGroup(summary.id)
            #expect(!detail.lags.isEmpty)
            #expect(detail.members.count == summary.memberCount)
        }
    }
}
