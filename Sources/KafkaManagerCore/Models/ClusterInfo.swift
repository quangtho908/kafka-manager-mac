/// What a cluster reports about itself; enough for a "Test connection" result.
public struct ClusterInfo: Hashable, Sendable {
    public let brokerCount: Int

    public init(brokerCount: Int) {
        self.brokerCount = brokerCount
    }
}
