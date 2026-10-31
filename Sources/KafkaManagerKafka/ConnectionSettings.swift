import Foundation
import KafkaManagerCore

/// librdkafka property names and values for one profile. Pure and broker-free so it can be tested; the password is
/// part of the result and therefore lives only as long as the value does.
struct ConnectionSettings {
    let properties: [(key: String, value: String)]
    /// Strings to strip from any diagnostic text.
    let secrets: [String]
    /// Host of the first bootstrap server, for "can't reach" messages.
    let primaryHost: String

    static let sslCAFile = "/etc/ssl/cert.pem"

    /// - Throws: `KafkaError.authenticationFailed` when the profile needs SASL credentials and has none.
    init(cluster: ClusterConnection, credentials: Credentials?, caFileExists: Bool = FileManager.default.fileExists(atPath: sslCAFile)) throws {
        var properties: [(String, String)] = [
            ("bootstrap.servers", cluster.bootstrapServers),
            ("client.id", "kafka-manager"),
            // Reading must never create a topic as a side effect.
            ("allow.auto.create.topics", "false"),
            ("socket.keepalive.enable", "true"),
            ("log.connection.close", "false"),
        ]
        var secrets: [String] = []
        switch cluster.security {
        case .plaintext: properties.append(("security.protocol", "PLAINTEXT"))
        case .ssl: properties.append(("security.protocol", "SSL"))
        case .saslPlaintext: properties.append(("security.protocol", "SASL_PLAINTEXT"))
        case .saslSsl: properties.append(("security.protocol", "SASL_SSL"))
        }
        if cluster.security == .ssl || cluster.security == .saslSsl, caFileExists {
            properties.append(("ssl.ca.location", Self.sslCAFile))
        }
        if cluster.security == .saslPlaintext || cluster.security == .saslSsl {
            let username = credentials?.username ?? cluster.username
            guard let username, !username.isEmpty, let password = credentials?.password else { throw KafkaError.authenticationFailed }
            let mechanism: String
            switch cluster.saslMechanism ?? .plain {
            case .plain: mechanism = "PLAIN"
            case .scramSha256: mechanism = "SCRAM-SHA-256"
            case .scramSha512: mechanism = "SCRAM-SHA-512"
            }
            properties.append(("sasl.mechanism", mechanism))
            properties.append(("sasl.username", username))
            properties.append(("sasl.password", password))
            secrets = [password, username]
        }
        self.properties = properties
        self.secrets = secrets
        let first = cluster.bootstrapServers.split(separator: ",").first.map(String.init) ?? cluster.bootstrapServers
        primaryHost = first.trimmingCharacters(in: .whitespaces)
    }
}
