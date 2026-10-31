import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private func lag(_ topic: String, _ partition: Int, committed: Int64?, start: Int64 = 0, end: Int64) -> PartitionLag {
    PartitionLag(topic: topic, partition: partition, committedOffset: committed, startOffset: start, endOffset: end)
}

private let sample = GroupDetail(
    id: "billing", state: "Stable",
    members: [
        GroupMemberInfo(id: "m-b", clientId: "zeta", host: "/10.0.0.2", assignments: ["orders": [1], "payments": [0, 2]]),
        GroupMemberInfo(id: "m-a", clientId: "alpha", host: "/10.0.0.1", assignments: ["orders": [0]]),
        GroupMemberInfo(id: "m-idle", clientId: "alpha", host: "/10.0.0.3"),
    ],
    lags: [
        lag("payments", 2, committed: nil, start: 10, end: 60),
        lag("orders", 1, committed: 90, end: 100),
        lag("orders", 0, committed: 120, end: 100),
        lag("payments", 0, committed: 0, end: 20_000),
    ],
    coordinatorId: 2, coordinatorHost: "broker-2:9092", assignor: "range")

@Suite("Group detail presentation")
struct GroupDetailPresentationTests {
    @Test func headerSummarisesTotalsCoordinatorAndWarnings() {
        let header = GroupDetailPresentation.header(sample)
        #expect(header.totalLag == 10 + 50 + 0 + 20_000)
        #expect(header.totalLagShort == "20K")
        #expect(header.totalLagExact.contains("060") || header.totalLagExact.contains("20"))
        #expect(header.severity == .high)
        #expect(header.coordinatorText == "broker 2 (broker-2:9092)")
        #expect(header.assignorText == "range")
        #expect(header.memberCount == 3)
        #expect(header.uncommittedPartitions == 1 && header.aheadOfEndPartitions == 1 && header.warningCount == 2)
        #expect(header.warningText == "1 partition without a committed offset, 1 partition committed past the end")
    }

    @Test func missingCoordinatorAndAssignorShowDashes() {
        let header = GroupDetailPresentation.header(GroupDetail(id: "g", state: "Empty", members: [], lags: []))
        #expect(header.coordinatorText == "—" && header.assignorText == "—")
        #expect(header.totalLag == 0 && header.severity == .none && header.warningText == nil)
    }

    @Test func lagSectionsAreGroupedByTopicWithTotalsAndPartitionOrder() {
        let sections = GroupDetailPresentation.lagSections(sample)
        #expect(sections.map(\.topic) == ["orders", "payments"])
        #expect(sections[0].partitions.map(\.partition) == [0, 1])
        #expect(sections[0].header.lag == 10 && sections[1].header.lag == 20_050)
        #expect(sections[0].header.kind == .topic && sections[0].partitions.allSatisfy { $0.kind == .partition })
        #expect(sections[1].header.severity == .high)
    }

    @Test func noOffsetAndAheadOfEndAreExplained() {
        let sections = GroupDetailPresentation.lagSections(sample)
        let ahead = sections[0].partitions[0]
        #expect(ahead.isAheadOfEnd && ahead.lag == 0 && ahead.note?.contains("past the end") == true)
        let none = sections[1].partitions[1]
        #expect(none.committedText == "no offset" && none.hasNoOffset && none.lag == 50)
        #expect(none.note?.contains("end − start") == true)
        let plain = sections[0].partitions[1]
        #expect(plain.committedText == "90" && plain.note == nil && plain.severity == .low)
    }

    @Test func memberColumnNamesTheHolderOrDash() {
        let rows = GroupDetailPresentation.lagSections(sample).flatMap(\.partitions)
        func member(_ id: String) -> String? { rows.first { $0.id == id }?.memberText }
        #expect(member("orders/0") == "alpha" && member("orders/1") == "zeta")
        #expect(member("payments/2") == "zeta" && member("payments/0") == "zeta")
        let unassigned = GroupDetail(id: "g", state: "Empty", members: [], lags: [lag("t", 0, committed: 1, end: 5)])
        let row = GroupDetailPresentation.lagSections(unassigned)[0].partitions[0]
        #expect(row.memberText == "—" && row.memberHelp == nil)
        #expect(rows.first { $0.id == "orders/1" }?.memberHelp == "m-b")
    }

    @Test func emptyGroupWithOffsetsStillShowsLag() {
        let detail = GroupDetail(id: "g", state: "Empty", members: [], lags: [lag("t", 0, committed: 1, end: 5)])
        #expect(GroupDetailPresentation.lagSections(detail)[0].header.lag == 4)
        #expect(GroupDetailPresentation.header(detail).totalLag == 4)
    }

    @Test func membersAreSortedByClientIdAndCountPartitions() {
        let rows = GroupDetailPresentation.memberRows(sample)
        #expect(rows.map(\.id) == ["m-a", "m-idle", "m-b"])
        #expect(rows[0].partitionCount == 1 && rows[0].partitionsText == "orders: 0")
        #expect(rows[1].partitionCount == 0 && rows[1].partitionsText == "—")
        #expect(rows[2].partitionCount == 3 && rows[2].partitionsFull == "orders: 1; payments: 0, 2")
    }

    @Test func longPartitionListsAreCutButKeptWholeForTheTooltip() {
        let many = GroupMemberInfo(id: "m", clientId: "c", host: "h", assignments: ["a-long-topic-name": Array(0..<40)])
        let row = GroupDetailPresentation.memberRows(GroupDetail(id: "g", state: "Stable", members: [many], lags: []))[0]
        #expect(row.partitionsText.hasSuffix("…") && row.partitionsText.count <= GroupDetailPresentation.maxPartitionsTextLength + 1)
        #expect(row.partitionsFull.count > row.partitionsText.count)
    }

    @Test func severityPresentationHasTextAndSymbolForEveryLevel() {
        let all = LagSeverity.allCases.map(LagSeverityPresentation.init)
        #expect(all.allSatisfy { !$0.label.isEmpty && !$0.symbolName.isEmpty })
        #expect(Set(all.map(\.label)).count == 4 && Set(all.map(\.symbolName)).count == 4)
        #expect(LagSeverityPresentation(.none).tone == .good && LagSeverityPresentation(.high).tone == .critical)
    }
}

@MainActor
private struct DetailRig {
    let fake: FakeKafkaClient
    let session: ClusterSession
    let sleeper = SleepController()
    var goneCalls: GoneRecorder

    @MainActor final class GoneRecorder { var ids: [String] = [] }

    init(_ groups: [GroupDetail] = [sample]) {
        let fake = FakeKafkaClient(groups: groups)
        self.fake = fake
        session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(), factory: { _ in fake })
        goneCalls = GoneRecorder()
    }

    func model(_ id: String = "billing") -> GroupDetailViewModel {
        let recorder = goneCalls
        return GroupDetailViewModel(session: session, groupId: id, sleep: sleeper.sleep) { recorder.ids.append($0) }
    }
}

@Suite("GroupDetailViewModel") @MainActor
struct GroupDetailViewModelTests {
    @Test func nothingHappensAtInitAndLoadIsOneCall() async {
        let rig = DetailRig()
        let model = rig.model()
        #expect(model.state == .idle && rig.session.status == .idle)
        await model.loadIfNeeded()
        #expect(model.state.value?.id == "billing")
        #expect(model.header?.memberCount == 3)
        #expect(model.lagSections.count == 2 && model.memberRows.count == 3)
    }

    @Test func loadIfNeededDoesNotReloadLoadedData() async {
        let rig = DetailRig()
        let model = rig.model()
        await model.loadIfNeeded()
        await rig.fake.failing(.describeGroup, with: .timeout)
        await model.loadIfNeeded()
        #expect(model.state.value != nil)
    }

    @Test func refreshKeepsOldDataWhileRunningAndFailureReplacesIt() async {
        let rig = DetailRig()
        let model = rig.model()
        await model.loadIfNeeded()
        await rig.fake.setLatency(.seconds(5))
        let task = Task { await model.refresh() }
        await expectSettles { model.isRefreshing }
        #expect(model.state.value?.id == "billing")
        task.cancel()
        await task.value
        #expect(!model.isRefreshing && model.state.value?.id == "billing")
        await rig.fake.setLatency(.zero)
        await rig.fake.failing(.describeGroup, with: .timeout)
        await model.refresh()
        #expect(model.state.failureMessage == KafkaError.timeout.userMessage)
    }

    @Test func unknownGroupSaysSoAndReportsOnce() async {
        let rig = DetailRig()
        let model = rig.model("deleted-group")
        await model.loadIfNeeded()
        #expect(model.state.failureMessage == "The group deleted-group no longer exists. It may have been deleted.")
        #expect(rig.goneCalls.ids == ["deleted-group"])
    }

    @Test func otherErrorsDoNotClaimTheGroupIsGone() async {
        let rig = DetailRig()
        await rig.fake.failing(.describeGroup, with: .timeout)
        let model = rig.model()
        await model.loadIfNeeded()
        #expect(rig.goneCalls.ids.isEmpty)
    }

    @Test func cancelledFirstLoadGoesBackToIdle() async {
        let rig = DetailRig()
        _ = try? await rig.session.ensureConnected()
        await rig.fake.setLatency(.seconds(5))
        let model = rig.model()
        let task = Task { await model.loadIfNeeded() }
        await expectSettles { model.state.isLoading }
        task.cancel()
        await task.value
        #expect(model.state == .idle)
    }
}

@Suite("Group auto refresh") @MainActor
struct GroupAutoRefreshTests {
    @Test func offByDefaultAndNoSleepIsRequested() async {
        let rig = DetailRig()
        let model = rig.model()
        #expect(!model.autoRefreshOn && model.autoRefreshInterval == .ten)
        await model.autoRefresh()
        #expect(await rig.sleeper.snapshot().requested == 0)
    }

    @Test func ticksRefreshUsingTheChosenIntervalAndStopWhenSwitchedOff() async {
        let rig = DetailRig()
        let model = rig.model()
        await model.loadIfNeeded()
        model.autoRefreshOn = true
        model.autoRefreshInterval = .five
        let loop = Task { @MainActor in await model.autoRefresh() }
        await expectSettles { await rig.sleeper.pendingCount == 1 }
        #expect(await rig.sleeper.requested == [.seconds(5)])
        await rig.fake.failing(.describeGroup, with: nil)
        await rig.sleeper.fire(0)
        await expectSettles { await rig.sleeper.snapshot().requested == 2 }
        model.autoRefreshOn = false
        await expectSettles { await rig.sleeper.pendingCount == 1 }
        await rig.sleeper.fire(1)
        await loop.value
        #expect(await rig.sleeper.snapshot().requested == 2, "no further sleep after the toggle went off")
    }

    @Test func cancellingTheLoopEndsItWithoutRefreshing() async {
        let rig = DetailRig()
        let model = rig.model()
        await model.loadIfNeeded()
        await rig.fake.failing(.describeGroup, with: .timeout)
        model.autoRefreshOn = true
        let loop = Task { @MainActor in await model.autoRefresh() }
        await expectSettles { await rig.sleeper.pendingCount == 1 }
        loop.cancel()
        await loop.value
        #expect(model.state.value != nil, "a cancelled loop must not have refreshed")
    }

    @Test func aDisconnectedSessionTurnsAutoRefreshOff() async {
        let rig = DetailRig()
        let model = rig.model()
        await model.loadIfNeeded()
        model.autoRefreshOn = true
        let loop = Task { @MainActor in await model.autoRefresh() }
        await expectSettles { await rig.sleeper.pendingCount == 1 }
        await rig.session.disconnect()
        await rig.sleeper.fire(0)
        await loop.value
        #expect(!model.autoRefreshOn)
        #expect(rig.session.status != .connected)
    }

    @Test func aFailedRefreshTurnsAutoRefreshOff() async {
        let rig = DetailRig()
        let model = rig.model()
        await model.loadIfNeeded()
        model.autoRefreshOn = true
        let loop = Task { @MainActor in await model.autoRefresh() }
        await expectSettles { await rig.sleeper.pendingCount == 1 }
        await rig.fake.failing(.describeGroup, with: .timeout)
        await rig.sleeper.fire(0)
        await loop.value
        #expect(!model.autoRefreshOn && model.state.failureMessage != nil)
    }

    @Test func keyChangesWithToggleAndInterval() {
        let model = DetailRig().model()
        let off = model.autoRefreshKey
        model.autoRefreshOn = true
        #expect(model.autoRefreshKey != off)
        let ten = model.autoRefreshKey
        model.autoRefreshInterval = .thirty
        #expect(model.autoRefreshKey != ten)
        #expect(AutoRefreshInterval.allCases.map(\.rawValue) == [5, 10, 30])
    }
}

@Suite("Topic to groups") @MainActor
struct TopicGroupsTests {
    private func rig(_ groups: [GroupDetail]) -> (FakeKafkaClient, ClusterSession) {
        let fake = FakeKafkaClient(groups: groups)
        let session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(), factory: { _ in fake })
        return (fake, session)
    }

    @Test func keepsOnlyGroupsThatUseTheTopicWithThisTopicsLag() async {
        let groups = [
            sample,
            GroupDetail(id: "other", state: "Stable", members: [], lags: [lag("inventory", 0, committed: 1, end: 3)]),
            GroupDetail(id: "assigned-only", state: "Stable",
                        members: [GroupMemberInfo(id: "m", clientId: "c", host: "h", assignments: ["orders": [0]])], lags: []),
        ]
        let (_, session) = rig(groups)
        let model = TopicGroupsViewModel(session: session, topicName: "orders")
        await model.scanGroups()
        #expect(model.rows.map(\.groupId) == ["assigned-only", "billing"])
        #expect(model.rows[1].lag == 10 && model.rows[0].lag == 0)
        #expect(model.scan == .finished(total: 3))
        #expect(model.progressText == "Checked 3 groups")
    }

    @Test func noGroupsMeansFinishedWithNoRows() async {
        let (_, session) = rig([])
        let model = TopicGroupsViewModel(session: session, topicName: "orders")
        await model.scanGroups()
        #expect(model.rows.isEmpty && model.scan == .finished(total: 0))
    }

    @Test func listFailureIsReported() async {
        let (fake, session) = rig([sample])
        await fake.failing(.listGroups, with: .timeout)
        let model = TopicGroupsViewModel(session: session, topicName: "orders")
        await model.scanGroups()
        #expect(model.scan == .failed(message: KafkaError.timeout.userMessage))
    }

    @Test func unreadableGroupsAreCountedNotFatal() async {
        let (fake, session) = rig([sample, GroupDetail(id: "z", state: "Empty", members: [], lags: [])])
        await fake.failing(.describeGroup, with: .clusterError(code: 30, message: "denied"))
        let model = TopicGroupsViewModel(session: session, topicName: "orders")
        await model.scanGroups()
        #expect(model.unreadableCount == 2 && model.rows.isEmpty && model.scan == .finished(total: 2))
    }

    @Test func fortySampleGroupsScanQuicklyAndFindOrdersConsumers() async {
        let fake = FakeKafkaClient.sampleData(topicCount: 10)
        let session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(), factory: { _ in fake })
        let model = TopicGroupsViewModel(session: session, topicName: "orders")
        let clock = ContinuousClock()
        let elapsed = await clock.measure { await model.scanGroups() }
        #expect(elapsed < .seconds(2))
        #expect(model.scan == .finished(total: FakeKafkaClient.sampleGroupCount))
        #expect(model.rows.contains { $0.groupId == "orders-service" })
        #expect(!model.rows.contains { $0.groupId == "payments-service" })
        #expect(model.rows.map(\.groupId) == model.rows.map(\.groupId).sorted())
    }

    @Test func cancellingTheScanStopsItAndKeepsWhatWasFound() async {
        let fake = FakeKafkaClient.sampleData(topicCount: 10)
        let session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(), factory: { _ in fake })
        _ = try? await session.ensureConnected()
        await fake.setLatency(.milliseconds(20))
        let model = TopicGroupsViewModel(session: session, topicName: "orders")
        let task = Task { await model.scanGroups() }
        await expectSettles { model.isScanning }
        try? await Task.sleep(for: .milliseconds(60))
        task.cancel()
        await task.value
        #expect(model.scan == .idle)
    }
}

@Suite("Group list lag and shell jump") @MainActor
struct GroupListLagTests {
    @Test func loadsLagForVisibleGroupsOnlyAndReportsProgress() async {
        let fake = FakeKafkaClient.sampleData(topicCount: 10)
        let session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(), factory: { _ in fake })
        let model = GroupListViewModel(session: session)
        await model.loadIfNeeded()
        #expect(model.lagTotals.isEmpty && model.lagStatusText == "")
        model.searchText = "sample-group-0"
        let visible = model.visibleGroups.count
        await model.loadVisibleLag()
        #expect(model.lagTotals.count == visible)
        #expect(model.lagStatusText == "Lag loaded for \(visible) of \(visible) groups")
        #expect(model.lagTotals["orders-service"] == nil)
        model.searchText = ""
        #expect(model.lagStatusText == "Lag loaded for \(visible) of 40 groups")
        await model.loadVisibleLag()
        #expect(model.lagTotals.count == 40)
        // orders-service: 5 behind on p0, caught up on p1, never committed on p2 (30 retained).
        #expect(model.lagTotals["orders-service"] == 35)
    }

    @Test func reloadingTheListClearsCachedLag() async {
        let fake = FakeKafkaClient.sampleData(topicCount: 10)
        let session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(), factory: { _ in fake })
        let model = GroupListViewModel(session: session)
        await model.loadIfNeeded()
        await model.loadVisibleLag()
        #expect(!model.lagTotals.isEmpty)
        await model.refresh()
        #expect(model.lagTotals.isEmpty)
    }

    @Test func detailModelIsCachedPerGroupAndRemovalRefreshesTheList() async {
        let fake = FakeKafkaClient(groups: [sample])
        let session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(), factory: { _ in fake })
        let model = GroupListViewModel(session: session)
        await model.loadIfNeeded()
        let detail = model.detailModel(for: "billing")
        #expect(model.detailModel(for: "billing") === detail)
        #expect(model.detailModel(for: "other") !== detail)
        model.selection = "ghost"
        let ghost = model.detailModel(for: "ghost")
        await ghost.loadIfNeeded()
        #expect(model.selection == nil, "the list reloaded once and dropped the vanished selection")
        #expect(model.state.value?.map(\.id) == ["billing"])
    }

    @Test func openingAGroupFromATopicSwitchesSectionAndSelects() async {
        let fake = FakeKafkaClient(groups: [sample])
        let connection = ClusterConnection(name: "c", bootstrapServers: "h:9092")
        let session = ClusterSession(connection: connection, secrets: InMemorySecretStore(), factory: { _ in fake })
        let connections = ConnectionsViewModel(store: InMemoryConnectionStore(connections: [connection]), secrets: InMemorySecretStore())
        await connections.load()
        connections.selection = connection.id
        let shell = ShellViewModel(connections: connections)
        shell.selectedSection = .topics
        let topics = shell.topicList(for: session)
        topics.detailModel(for: "orders").openGroup("billing")
        #expect(shell.selectedSection == .groups)
        #expect(shell.groupList(for: session).selection == "billing")
        #expect(shell.selectedGroupList?.selection == "billing")
    }
}
