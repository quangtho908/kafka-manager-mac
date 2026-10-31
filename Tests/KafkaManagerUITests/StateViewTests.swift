import Testing
@testable import KafkaManagerUI

@Suite("StateBranch")
struct StateBranchTests {
    private func branch(_ state: LoadState<[Int]>) -> StateBranch {
        StateBranch.resolve(state, isEmpty: \.isEmpty)
    }

    @Test func eachStateMapsToItsBranch() {
        #expect(branch(.idle) == .idle)
        #expect(branch(.loading) == .loading)
        #expect(branch(.loaded([1])) == .content)
        #expect(branch(.loaded([])) == .empty)
        #expect(branch(.failed(message: "no")) == .failed(message: "no", details: nil))
        #expect(branch(.failed(message: "no", details: "code 7")) == .failed(message: "no", details: "code 7"))
    }

    @Test func isEmptyDecidesBetweenEmptyAndContent() {
        #expect(StateBranch.resolve(LoadState.loaded(5), isEmpty: { $0 == 0 }) == .content)
        #expect(StateBranch.resolve(LoadState.loaded(0), isEmpty: { $0 == 0 }) == .empty)
        #expect(StateBranch.resolve(LoadState.loaded("x"), isEmpty: { _ in false }) == .content)
    }

    @Test func isEmptyIsNotConsultedUnlessLoaded() {
        var calls = 0
        for state in [LoadState<Int>.idle, .loading, .failed(message: "x")] {
            _ = StateBranch.resolve(state) { _ in calls += 1; return false }
        }
        #expect(calls == 0)
    }
}

@Suite("LoadState details")
struct LoadStateDetailsTests {
    @Test func failedDetailsDefaultToNilAndKeepExistingBehaviour() {
        let plain = LoadState<Int>.failed(message: "m")
        #expect(plain.failureMessage == "m")
        #expect(plain.failureDetails == nil)
        #expect(plain == .failed(message: "m", details: nil))
        #expect(plain != .failed(message: "m", details: "d"))
    }

    @Test func detailsAreExposed() {
        let state = LoadState<Int>.failed(message: "m", details: "d")
        #expect(state.failureDetails == "d")
        #expect(LoadState<Int>.loaded(1).failureDetails == nil)
        #expect(LoadState<Int>.idle.failureMessage == nil)
    }
}
