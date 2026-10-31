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

    public enum TestState: Sendable, Equatable {
        case idle
        case testing
        case success(brokerCount: Int)
        case failure(message: String)

        /// Text for the result line; nil while idle or testing.
        public var message: String? {
            switch self {
            case .idle, .testing: nil
            case .success(let count): String(localized: "Connected. \(count) broker(s).")
            case .failure(let message): message
            }
        }
    }

    public let mode: Mode
    public let connectionID: UUID

    // Any edit to a field the test reads clears its result and discards one still in flight.
    public var name: String { didSet { resetTest() } }
    public var bootstrapServers: String { didSet { resetTest() } }
    public var environment: ClusterConnection.Environment { didSet { resetTest() } }
    public var security: ClusterConnection.Security { didSet { resetTest() } }
    public var saslMechanism: ClusterConnection.SASLMechanism? { didSet { resetTest() } }
    public var username: String { didSet { resetTest() } }
    /// What the user typed. Blank means "leave the stored password alone".
    public var password = "" { didSet { resetTest() } }
    public var clearStoredPassword = false { didSet { resetTest() } }

    /// Whether the Keychain holds a password for the profile being edited. False until `loadStoredPasswordFlag()` runs.
    public private(set) var hasStoredPassword = false
    public private(set) var saveState = SaveState.idle
    public private(set) var testState = TestState.idle

    @ObservationIgnored private let store: any ConnectionStore
    @ObservationIgnored private let secrets: any SecretStore
    @ObservationIgnored private let existingNames: [String]
    @ObservationIgnored private let tester: (any ConnectionTester)?
    /// Bumped by every edit and every new test, so a result from an older test is dropped.
    @ObservationIgnored private var testGeneration = 0

    /// - Parameter existingNames: names of the other profiles; the caller leaves out the profile being edited.
    /// - Parameter tester: enables "Test connection"; without one the form can't test.
    public init(
        mode: Mode,
        store: any ConnectionStore,
        secrets: any SecretStore,
        existingNames: [String],
        tester: (any ConnectionTester)? = nil
    ) {
        self.tester = tester
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

    // MARK: Test connection

    /// A valid draft, a tester, and neither a test nor a save already running.
    public var canTest: Bool {
        guard tester != nil, testState != .testing, saveState != .saving else { return false }
        return !issues.contains { $0.severity == .error }
    }

    /// Tests the current, unsaved values. The typed password is used if there is one; otherwise, when editing, the
    /// saved one is read now. The password is never kept; nothing is written. Cancelling the calling task ends the
    /// test and returns to idle.
    public func testConnection() async {
        guard canTest, let tester, case .success(let servers) = BootstrapServers.parse(bootstrapServers) else { return }
        testGeneration += 1
        let ticket = testGeneration
        testState = .testing

        let usesSASL = usesSASL
        let draft = ClusterConnection(
            id: connectionID,
            name: name.trimmingCharacters(in: .whitespacesAndNewlines),
            bootstrapServers: servers.normalized,
            environment: environment,
            security: security,
            saslMechanism: usesSASL ? saslMechanism : nil,
            username: usesSASL ? username.trimmingCharacters(in: .whitespacesAndNewlines) : nil
        )
        let outcome: TestState
        do {
            let credentials = try await testCredentials(for: draft)
            outcome = .success(brokerCount: try await tester.test(draft, credentials: credentials))
        } catch {
            outcome = Task.isCancelled || error is CancellationError ? .idle : .failure(message: Self.testMessage(for: error))
        }
        guard ticket == testGeneration else { return }
        testState = Task.isCancelled ? .idle : outcome
    }

    private func resetTest() {
        testGeneration += 1
        if testState != .idle { testState = .idle }
    }

    private func testCredentials(for draft: ClusterConnection) async throws -> Credentials? {
        guard usesSASL, let username = draft.username else { return nil }
        if !password.isEmpty { return Credentials(username: username, password: password) }
        var saved: String?
        if isEditing && !clearStoredPassword { saved = try await secrets.password(for: connectionID) }
        return Credentials(username: username, password: saved ?? "")
    }

    private static func testMessage(for error: any Error) -> String {
        switch error {
        case let error as KafkaError: error.userMessage
        case let error as SecretStoreError: error.userMessage
        default: String(localized: "The connection test failed.")
        }
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
