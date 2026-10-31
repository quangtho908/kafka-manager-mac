import os

/// Live count of the C objects this module owns, per kind. Every wrapper increments on creation and decrements when it
/// frees the object, so a leak (or a double free) shows up as a count that does not return to zero. Tests read it.
public enum ResourceKind: String, Sendable, CaseIterable {
    case conf, client, metadata, queue, event, adminOptions, topic, adminObject
}

public enum ResourceCounters {
    private static let counts = OSAllocatedUnfairLock(initialState: [ResourceKind: Int]())

    static func acquired(_ kind: ResourceKind) { counts.withLock { $0[kind, default: 0] += 1 } }
    static func released(_ kind: ResourceKind) { counts.withLock { $0[kind, default: 0] -= 1 } }

    public static func live(_ kind: ResourceKind) -> Int { counts.withLock { $0[kind] ?? 0 } }

    /// Kinds whose count is not zero, for test failure messages.
    public static var outstanding: [ResourceKind: Int] {
        counts.withLock { $0.filter { $0.value != 0 } }
    }
}
