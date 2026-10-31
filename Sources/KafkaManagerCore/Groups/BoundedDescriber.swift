import Foundation

/// Describes many groups with at most `limit` calls in flight, so a cluster with hundreds of groups is never hit
/// with hundreds of requests at once.
public enum BoundedDescriber {
    public static let defaultLimit = 4

    public struct Item: Sendable {
        public let index: Int
        public let id: String
        public let result: Result<GroupDetail, KafkaError>
    }

    /// Calls `describe` for each id and hands every outcome to `deliver` as it completes (in completion order; `index`
    /// is the position in `ids`). A cancelled caller starts no further calls and returns once the running ones end.
    /// An error that is not a `KafkaError` is reported as `.clusterError(code: -1, …)` without details.
    public static func run(
        ids: [String],
        limit: Int = defaultLimit,
        describe: @escaping @Sendable (String) async throws -> GroupDetail,
        deliver: @MainActor (Item) -> Void
    ) async {
        let width = max(limit, 1)
        await withTaskGroup(of: Item?.self) { group in
            var next = 0
            func startNext(_ group: inout TaskGroup<Item?>) {
                guard next < ids.count, !Task.isCancelled else { return }
                let index = next, id = ids[index]
                next += 1
                group.addTask {
                    if Task.isCancelled { return nil }
                    do {
                        return Item(index: index, id: id, result: .success(try await describe(id)))
                    } catch is CancellationError {
                        return nil
                    } catch let error as KafkaError {
                        return Item(index: index, id: id, result: .failure(error))
                    } catch {
                        return Item(index: index, id: id, result: .failure(.clusterError(code: -1, message: "describe failed")))
                    }
                }
            }
            for _ in 0..<min(width, ids.count) { startNext(&group) }
            while let finished = await group.next() {
                if let finished, !Task.isCancelled { await deliver(finished) }
                startNext(&group)
            }
        }
    }
}
