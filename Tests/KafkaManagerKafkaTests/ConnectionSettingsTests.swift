import KafkaManagerCore
import Testing
@testable import KafkaManagerKafka

struct ConnectionSettingsTests {
    private func settings(
        _ security: ClusterConnection.Security, mechanism: ClusterConnection.SASLMechanism? = nil,
        credentials: Credentials? = nil, ca: Bool = true
    ) throws -> ConnectionSettings {
        let cluster = ClusterConnection(
            name: "c", bootstrapServers: "kafka-1:9092, kafka-2:9092", security: security, saslMechanism: mechanism, username: "stored-user")
        return try ConnectionSettings(cluster: cluster, credentials: credentials, caFileExists: ca)
    }

    private func value(_ key: String, in settings: ConnectionSettings) -> String? {
        settings.properties.first { $0.key == key }?.value
    }

    @Test func plaintext() throws {
        let s = try settings(.plaintext)
        #expect(value("security.protocol", in: s) == "PLAINTEXT")
        #expect(value("bootstrap.servers", in: s) == "kafka-1:9092, kafka-2:9092")
        #expect(value("allow.auto.create.topics", in: s) == "false")
        #expect(value("sasl.password", in: s) == nil)
        #expect(s.secrets.isEmpty)
        #expect(s.primaryHost == "kafka-1:9092")
    }

    @Test func sslUsesTheSystemCAFileWhenPresent() throws {
        #expect(value("ssl.ca.location", in: try settings(.ssl)) == ConnectionSettings.sslCAFile)
        #expect(value("ssl.ca.location", in: try settings(.ssl, ca: false)) == nil)
    }

    @Test(arguments: [
        (ClusterConnection.SASLMechanism.plain, "PLAIN"),
        (.scramSha256, "SCRAM-SHA-256"),
        (.scramSha512, "SCRAM-SHA-512"),
    ])
    func saslMechanisms(mechanism: ClusterConnection.SASLMechanism, expected: String) throws {
        let s = try settings(.saslSsl, mechanism: mechanism, credentials: Credentials(username: "u", password: "p"))
        #expect(value("security.protocol", in: s) == "SASL_SSL")
        #expect(value("sasl.mechanism", in: s) == expected)
        #expect(value("sasl.username", in: s) == "u")
        #expect(value("sasl.password", in: s) == "p")
        #expect(Set(s.secrets) == ["u", "p"])
    }

    @Test func saslWithoutCredentialsIsAnAuthenticationFailure() {
        #expect(throws: KafkaError.authenticationFailed) { try settings(.saslPlaintext, mechanism: .plain) }
    }
}
