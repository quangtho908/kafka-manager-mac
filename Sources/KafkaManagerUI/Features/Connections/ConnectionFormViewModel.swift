import Foundation
import Observation
import KafkaManagerCore

/// State and save logic for the add/edit/duplicate connection form. Holds no Kafka client and no password value
/// beyond what the user is typing; the typed password is cleared once a save succeeds.
@MainActor @Observable
public final class ConnectionFormViewModel {
    public enum Mode: Sendable, Equatable {
        case new
        case edit(ClusterConnection)
        case duplicate(ClusterConnection)
    }

    public enum SaveState: Sendable, Equatable {
        case idle
        case saving
        case saved(ClusterConnection)
        case failed(message: String)
    }

    public let mode: Mode
    public let connectionID: UUID

    public var name: String
    public var bootstrapServers: String
    public var environment: ClusterConnection.Environment
    public var security: ClusterConnection.Security
    public var saslMechanism: ClusterConnection.SASLMechanism?
    public var username: String
    /// What the user typed. Blank means "leave the stored password alone".
    public var password = ""
    public var clearStoredPassword = false

    /// Whether the Keychain holds a password for the profile being edited. False until `loadStoredPasswordFlag()` runs.
    public private(set) var hasStoredPassword = false
    public private(set) var saveState = SaveState.idle

    @ObservationIgnored private let store: any ConnectionStore
    @ObservationIgnored private let secrets: any SecretStore
    @ObservationIgnored private let existingNames: [String]

    /// - Parameter existingNames: names of the other profiles; the caller leaves out the profile being edited.
    public init(mode: Mode, store: any ConnectionStore, secrets: any SecretStore, existingNames: [String]) {
        self.mode = mode
        self.store = store
        self.secrets = secrets
        self.existingNames = existingNames

        switch mode {
        case .new:
            connectionID = UUID()
            name = ""
            bootstrapServers = ""
            environment = .local
            security = .plaintext
            saslMechanism = nil
            username = ""
        case .edit(let connection):
            connectionID = connection.id
            name = connection.name
            bootstrapServers = connection.bootstrapServers
            environment = connection.environment
            security = connection.security
            saslMechanism = connection.saslMechanism
            username = connection.username ?? ""
        case .duplicate(let connection):
            connectionID = UUID()
            name = String(localized: "\(connection.name) copy")
            bootstrapServers = connection.bootstrapServers
            environment = connection.environment
            security = connection.security
            saslMechanism = connection.saslMechanism
            username = connection.username ?? ""
        }
    }

    public var isEditing: Bool {
        if case .edit = mode { true } else { false }
    }

    public var usesSASL: Bool {
        security == .saslPlaintext || security == .saslSsl
    }

    /// Recomputed from the current fields on every read.
    public var issues: [ValidationIssue] {
        ConnectionValidator.validate(
            name: name,
            bootstrapServers: bootstrapServers,
            security: security,
            saslMechanism: saslMechanism,
            username: username,
            existingNames: existingNames
        )
    }

    public var canSave: Bool {
        guard saveState != .saving else { return false }
        return !issues.contains { $0.severity == .error }
    }

    // MARK: Stored password flag

    /// Only checks whether a password exists; the value is dropped immediately. Does nothing unless editing.
    public func loadStoredPasswordFlag() async {
        guard isEditing else { return }
        let found: Bool
        do {
            found = try await secrets.password(for: connectionID) != nil
        } catch {
            found = false
        }
        guard !Task.isCancelled else { return }
        hasStoredPassword = found
    }

    // MARK: Save

    private enum SecretChange {
        case none
        case set(String)
        case delete
    }

    public func save() async {
        guard canSave else { return }
        guard case .success(let servers) = BootstrapServers.parse(bootstrapServers) else { return }
        saveState = .saving

        let usesSASL = usesSASL
        let trimmedUser = username.trimmingCharacters(in: .whitespacesAndNewlines)
        let connection = ClusterConnection(
            id: connectionID,
            name: name.trimmingCharacters(in: .whitespacesAndNewlines),
            bootstrapServers: servers.normalized,
            environment: environment,
            security: security,
            saslMechanism: usesSASL ? saslMechanism : nil,
            username: usesSASL ? trimmedUser : nil
        )

        let change = secretChange(usesSASL: usesSASL)
        var wroteSecret = false
        do {
            switch change {
            case .none: break
            case .set(let value):
                try await secrets.setPassword(value, for: connectionID)
                wroteSecret = true
            case .delete:
                try await secrets.deletePassword(for: connectionID)
            }
        } catch {
            saveState = .failed(message: Self.message(for: error))
            return
        }

        do {
            try await store.save(connection)
        } catch {
            if wroteSecret && !isEditing {
                try? await secrets.deletePassword(for: connectionID)
            }
            saveState = .failed(message: Self.message(for: error))
            return
        }

        switch change {
        case .set: hasStoredPassword = true
        case .delete: hasStoredPassword = false
        case .none: break
        }
        password = ""
        clearStoredPassword = false
        saveState = .saved(connection)
    }

    private func secretChange(usesSASL: Bool) -> SecretChange {
        guard usesSASL else { return isEditing ? .delete : .none }
        if !password.isEmpty { return .set(password) }
        if clearStoredPassword && isEditing { return .delete }
        return .none
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case let error as ConnectionStoreError: error.userMessage
        case let error as SecretStoreError: error.userMessage
        default: String(localized: "The connection couldn't be saved.")
        }
    }
}
