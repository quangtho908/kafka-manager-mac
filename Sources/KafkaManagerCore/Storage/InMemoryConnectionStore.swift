import Foundation

/// Non-persistent store for tests and previews.
public actor InMemoryConnectionStore: ConnectionStore {
    private var connections: [ClusterConnection]

    public init(connections: [ClusterConnection] = []) {
        self.connections = connections
    }

    public func load() async throws -> [ClusterConnection] {
        connections
    }

    public func save(_ connection: ClusterConnection) async throws {
        if let index = connections.firstIndex(where: { $0.id == connection.id }) {
            connections[index] = connection
        } else {
            connections.append(connection)
        }
    }

    public func delete(id: UUID) async throws {
        connections.removeAll { $0.id == id }
    }
}
