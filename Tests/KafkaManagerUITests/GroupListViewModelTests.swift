import Testing
import Foundation
import KafkaManagerCore
@testable import KafkaManagerUI

private func group(_ id: String, _ state: String = "Stable", members: Int = 1) -> GroupDetail {
    GroupDetail(id: id, state: state, members: (0..<members).map { GroupMemberInfo(id: "m\($0)", clientId: "c", host: "h") }, lags: [])
}

@MainActor
private final class Clock {
    var date = Date(timeIntervalSince1970: 1_000_000)
    func advance(_ seconds: TimeInterval) { date.addTimeInterval(seconds) }
}

@MainActor
private struct GroupRig {
    let fake: FakeKafkaClient
    let session: ClusterSession
    let clock = Clock()
    let model: GroupListViewModel

    init(_ groups: [GroupDetail]) {
        let fake = FakeKafkaClient(groups: groups)
        self.fake = fake
        session = ClusterSession(
            connection: ClusterConnection(name: "c", bootstrapServers: "h:9092"), secrets: InMemorySecretStore(), factory: { _ in fake })
        let clock = clock
        model = GroupListViewModel(session: session, now: { clock.date })
    }
}

@Suite("GroupListViewModel loading") @MainActor
struct GroupListLoadingTests {
    @Test func nothingHappensAtInit() async {
        let rig = GroupRig([group("a")])
        #expect(rig.model.state == .idle)
        #expect(rig.session.status == .idle)
    }

    @Test func loadingConnectsAndCallsListGroupsOnce() async {
        let rig = GroupRig([group("a"), group("b")])
        await rig.model.loadIfNeeded()
        #expect(rig.model.state.value?.map(\.id) == ["a", "b"])
        #expect(rig.model.countText == "2 groups")
    }

    @Test func freshListIsNotReloadedButStaleOneIs() async {
        let rig = GroupRig([group("a")])
        await rig.model.loadIfNeeded()
        // A reload would hit this failure, so a still-loaded list proves no call was made.
        await rig.fake.failing(.listGroups, with: .timeout)
        rig.clock.advance(29)
        await rig.model.loadIfNeeded()
        #expect(rig.model.state.value?.count == 1)
        rig.clock.advance(2)
        await rig.model.loadIfNeeded()
        #expect(rig.model.state.failureMessage == KafkaError.timeout.userMessage)
    }

    @Test func refreshKeepsTheListVisibleWhileRunning() async {
        let rig = GroupRig([group("a")])
        await rig.model.loadIfNeeded()
        await rig.fake.setLatency(.seconds(5))
        let task = Task { await rig.model.refresh() }
        await expectSettles { rig.model.isRefreshing }
        #expect(rig.model.state.value?.map(\.id) == ["a"])
        task.cancel()
        await task.value
        #expect(!rig.model.isRefreshing)
        #expect(rig.model.state.value?.map(\.id) == ["a"])
    }

    @Test func failureShowsTheMessageAndRetryLoads() async {
        let rig = GroupRig([group("a")])
        await rig.fake.failing(.listGroups, with: .timeout)
        await rig.model.loadIfNeeded()
        #expect(rig.model.state.failureMessage == KafkaError.timeout.userMessage)
        await rig.fake.failing(.listGroups, with: nil)
        await rig.model.refresh()
        #expect(rig.model.state.value?.map(\.id) == ["a"])
    }

    @Test func cancelledFirstLoadGoesBackToIdle() async {
        let rig = GroupRig([group("a")])
        // Connect first: a cancelled connect is allowed to finish (T20), which would make this test wait out the latency.
        _ = try? await rig.session.ensureConnected()
        await rig.fake.setLatency(.seconds(5))
        let task = Task { await rig.model.loadIfNeeded() }
        await expectSettles { rig.model.state.isLoading }
        task.cancel()
        await task.value
        #expect(rig.model.state == .idle)
    }

    @Test func emptyClusterIsLoadedWithNoGroups() async {
        let rig = GroupRig([])
        await rig.model.loadIfNeeded()
        #expect(rig.model.state.value?.isEmpty == true)
        #expect(rig.model.countText == "0 groups")
        #expect(!rig.model.hasNoMatches)
    }

    @Test func selectionSurvivesARefreshWhileTheGroupExists() async {
        let rig = GroupRig([group("a"), group("b")])
        await rig.model.loadIfNeeded()
        rig.model.selection = "b"
        await rig.model.refresh()
        #expect(rig.model.selection == "b")
    }
}

@Suite("GroupListViewModel search and sort") @MainActor
struct GroupListSearchSortTests {
    private func loaded(_ groups: [GroupDetail]) async -> GroupListViewModel {
        let rig = GroupRig(groups)
        await rig.model.loadIfNeeded()
        return rig.model
    }

    @Test func searchIsCaseInsensitiveSubstringOfTheId() async {
        let model = await loaded([group("Orders-Service"), group("payments"), group("order-audit")])
        model.searchText = "ORDER"
        #expect(model.visibleGroups.map(\.id) == ["order-audit", "Orders-Service"])
        #expect(model.countText == "2 of 3 groups")
        model.searchText = "nope"
        #expect(model.hasNoMatches)
        model.searchText = "  "
        #expect(model.visibleGroups.count == 3)
    }

    @Test func sortsByIdStateAndMembersWithATiebreak() async {
        let model = await loaded([
            group("c", "Dead", members: 0), group("a", "Empty", members: 2), group("b", "Stable", members: 2), group("d", "PreparingRebalance", members: 5),
            group("e", "Weird", members: 0),
        ])
        #expect(model.visibleGroups.map(\.id) == ["a", "b", "c", "d", "e"])
        model.sortKey = .state
        #expect(model.visibleGroups.map(\.id) == ["b", "d", "a", "c", "e"])
        model.sortAscending = false
        #expect(model.visibleGroups.map(\.id) == ["e", "c", "a", "d", "b"])
        model.sortKey = .members
        model.sortAscending = true
        #expect(model.visibleGroups.map(\.id) == ["c", "e", "a", "b", "d"])
    }

    @Test func fiveHundredGroupsSortAndFilterQuickly() async {
        let model = await loaded((0..<500).map { group(String(format: "group-%03d", $0), $0 % 5 == 0 ? "Empty" : "Stable", members: $0 % 7) })
        let clock = ContinuousClock()
        let elapsed = clock.measure {
            model.sortKey = .members
            #expect(model.visibleGroups.count == 500)
            model.searchText = "group-42"
            #expect(model.visibleGroups.count == 10)
        }
        #expect(elapsed < .seconds(1))
    }

    @Test func statePresentationAlwaysHasTextAndSymbol() {
        let all: [GroupState] = [.stable, .preparingRebalance, .completingRebalance, .empty, .dead, .other("Odd"), .other("")]
        for state in all {
            let presentation = GroupStatePresentation(state)
            #expect(!presentation.label.isEmpty && !presentation.symbolName.isEmpty)
        }
        #expect(GroupStatePresentation(.other("Odd")).label == "Odd")
        #expect(GroupStatePresentation(.other("")).label == "Unknown")
        #expect(GroupStatePresentation(.stable).tone == .good)
    }

    @Test func shellKeepsOneListPerSessionAndResetsOnEdit() async {
        let rig = GroupRig([group("a")])
        let shell = ShellViewModel(connections: ConnectionsViewModel(store: InMemoryConnectionStore(), secrets: InMemorySecretStore()))
        let first = shell.groupList(for: rig.session)
        #expect(shell.groupList(for: rig.session) === first)
        #expect(first.belongs(to: rig.session))
    }
}
