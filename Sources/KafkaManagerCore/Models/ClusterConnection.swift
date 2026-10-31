import Foundation

/// Saved connection profile for a Kafka cluster. Secrets live in the Keychain, not here.
public struct ClusterConnection: Identifiable, Codable, Hashable, Sendable {
    public enum Environment: String, Codable, Hashable, Sendable, CaseIterable {
        case local
        case staging
        case production
    }

    public enum Security: String, Codable, Hashable, Sendable, CaseIterable {
        case plaintext
        case ssl
        case saslPlaintext
        case saslSsl
    }

    public enum SASLMechanism: String, Codable, Hashable, Sendable, CaseIterable {
        case plain
        case scramSha256
        case scramSha512
    }

    public let id: UUID
    public var name: String
    public var bootstrapServers: String
    public var environment: Environment
    public var security: Security
    public var saslMechanism: SASLMechanism?
    public var username: String?

    public init(
        id: UUID = UUID(),
        name: String,
        bootstrapServers: String,
        environment: Environment = .local,
        security: Security = .plaintext,
        saslMechanism: SASLMechanism? = nil,
        username: String? = nil
    ) {
        self.id = id
        self.name = name
        self.bootstrapServers = bootstrapServers
        self.environment = environment
        self.security = security
        self.saslMechanism = saslMechanism
        self.username = username
    }

    /// Profiles saved before these fields existed decode as a local, plaintext cluster.
    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(UUID.self, forKey: .id)
        name = try container.decode(String.self, forKey: .name)
        bootstrapServers = try container.decode(String.self, forKey: .bootstrapServers)
        environment = try container.decodeIfPresent(Environment.self, forKey: .environment) ?? .local
        security = try container.decodeIfPresent(Security.self, forKey: .security) ?? .plaintext
        saslMechanism = try container.decodeIfPresent(SASLMechanism.self, forKey: .saslMechanism)
        username = try container.decodeIfPresent(String.self, forKey: .username)
    }
}
