import Testing
import Foundation
import os
@testable import KafkaManagerCore

private final class Counter: Sendable {
    private let state = OSAllocatedUnfairLock(initialState: (current: 0, peak: 0, started: 0))
    func begin() { state.withLock { $0.current += 1; $0.peak = max($0.peak, $0.current); $0.started += 1 } }
    func end() { state.withLock { $0.current -= 1 } }
    var peak: Int { state.withLock { $0.peak } }
    var started: Int { state.withLock { $0.started } }
}

private func detail(_ id: String) -> GroupDetail { GroupDetail(id: id, state: "Stable", members: [], lags: []) }

@Suite("BoundedDescriber")
struct BoundedDescriberTests {
    @Test func neverExceedsTheLimitAndDeliversEveryGroupOnce() async {
        let counter = Counter()
        let ids = (0..<40).map { "g\($0)" }
        var delivered: [BoundedDescriber.Item] = []
        await BoundedDescriber.run(ids: ids, limit: 4, describe: { id in
            counter.begin()
            defer { counter.end() }
            try await Task.sleep(for: .milliseconds(3))
            return detail(id)
        }, deliver: { delivered.append($0) })
        #expect(counter.peak <= 4 && counter.peak > 1)
        #expect(delivered.count == 40)
        #expect(Set(delivered.map(\.index)) == Set(0..<40))
        #expect(delivered.allSatisfy { ids[$0.index] == $0.id })
    }

    @Test func limitBelowOneStillRunsOneAtATime() async {
        let counter = Counter()
        var count = 0
        await BoundedDescriber.run(ids: ["a", "b", "c"], limit: 0, describe: { id in
            counter.begin(); defer { counter.end() }
            return detail(id)
        }, deliver: { _ in count += 1 })
        #expect(counter.peak == 1 && count == 3)
    }

    @Test func cancellingStopsStartingNewCalls() async {
        let counter = Counter()
        let ids = (0..<100).map { "g\($0)" }
        let task = Task {
            await BoundedDescriber.run(ids: ids, limit: 4, describe: { id in
                counter.begin(); defer { counter.end() }
                try await Task.sleep(for: .milliseconds(20))
                return detail(id)
            }, deliver: { _ in })
        }
        try? await Task.sleep(for: .milliseconds(30))
        task.cancel()
        await task.value
        #expect(counter.started < 100)
        #expect(counter.started <= 8)
    }

    @Test func failuresAreReportedPerGroupWithoutStoppingTheRest() async {
        var failures = 0, successes = 0
        await BoundedDescriber.run(ids: ["a", "bad", "c"], limit: 2, describe: { id in
            if id == "bad" { throw KafkaError.timeout }
            return detail(id)
        }, deliver: { item in
            if case .failure(let error) = item.result { #expect(error == .timeout); failures += 1 } else { successes += 1 }
        })
        #expect(failures == 1 && successes == 2)
    }

    @Test func noIdsDoesNothing() async {
        var count = 0
        await BoundedDescriber.run(ids: [], describe: { detail($0) }, deliver: { _ in count += 1 })
        #expect(count == 0)
    }

    @Test func unknownGroupCodeIsRecognised() {
        #expect(KafkaError.clusterError(code: 69, message: "x").isUnknownGroup)
        #expect(!KafkaError.clusterError(code: 3, message: "x").isUnknownGroup)
        #expect(!KafkaError.timeout.isUnknownGroup)
    }
}
