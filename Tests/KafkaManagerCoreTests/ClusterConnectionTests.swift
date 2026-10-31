import Testing
import Foundation
@testable import KafkaManagerCore

@Test func clusterConnectionRoundTripsThroughJSON() throws {
    let original = ClusterConnection(name: "local", bootstrapServers: "localhost:9092")
    let decoded = try JSONDecoder().decode(ClusterConnection.self, from: JSONEncoder().encode(original))
    #expect(decoded == original)
}

@Test func clusterConnectionRoundTripsNewFields() throws {
    let original = ClusterConnection(
        name: "prod",
        bootstrapServers: "a:9093,b:9093",
        environment: .production,
        security: .saslSsl,
        saslMechanism: .scramSha512,
        username: "alice"
    )
    let decoded = try JSONDecoder().decode(ClusterConnection.self, from: JSONEncoder().encode(original))
    #expect(decoded == original)
}

@Test func clusterConnectionDecodesOldJSONWithDefaults() throws {
    let id = UUID()
    let json = #"{"id":"\#(id.uuidString)","name":"old","bootstrapServers":"localhost:9092"}"#
    let decoded = try JSONDecoder().decode(ClusterConnection.self, from: Data(json.utf8))
    #expect(decoded.id == id)
    #expect(decoded.environment == .local)
    #expect(decoded.security == .plaintext)
    #expect(decoded.saslMechanism == nil)
    #expect(decoded.username == nil)
}

@Test func clusterConnectionJSONNeverContainsPassword() throws {
    let connection = ClusterConnection(name: "x", bootstrapServers: "h:1", security: .saslSsl, saslMechanism: .plain, username: "u")
    let json = String(decoding: try JSONEncoder().encode(connection), as: UTF8.self)
    #expect(!json.lowercased().contains("password"))
}
