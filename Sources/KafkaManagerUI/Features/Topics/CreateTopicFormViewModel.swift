import Foundation
import Observation
import KafkaManagerCore

public enum CleanupPolicy: String, CaseIterable, Identifiable, Sendable {
    case delete
    case compact
    case compactAndDelete = "compact,delete"

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .delete: String(localized: "Delete")
        case .compact: String(localized: "Compact")
        case .compactAndDelete: String(localized: "Compact and delete")
        }
    }
}

/// State and submit logic of the "New Topic" form. Validation messages for a field show once the user has touched it
/// (or tried to create); writes are refused up front when the cluster is read-only.
@MainActor @Observable
public final class CreateTopicFormViewModel {
    public enum Field: Hashable, Sendable { case name, partitions, replicationFactor, retention }

    public enum Issue: Hashable, Sendable {
        case error(Field, String)
        case warning(Field, String)

        public var field: Field {
            switch self {
            case .error(let field, _), .warning(let field, _): field
            }
        }

        public var message: String {
            switch self {
            case .error(_, let message), .warning(_, let message): message
            }
        }

        public var isError: Bool {
            if case .error = self { true } else { false }
        }
    }

    public enum SubmitState: Equatable, Sendable {
        case idle
        case creating
        case created(name: String)
        case failed(message: String)
    }

    public var name = ""
    public var partitions = 1
    public var replicationFactor = 1
    /// Blank leaves the broker default.
    public var retentionMs = ""
    /// nil leaves the broker default.
    public var cleanupPolicy: CleanupPolicy?

    public private(set) var submitState = SubmitState.idle
    public private(set) var touched = Set<Field>()

    @ObservationIgnored private let session: ClusterSession
    @ObservationIgnored private let existingNames: @MainActor () -> Set<String>
    @ObservationIgnored private let onCreated: @MainActor (String) async -> Void
    @ObservationIgnored private var submitTask: Task<Void, Never>?

    /// - Parameter existingNames: current topic names, read when validating, so a duplicate is caught before the call.
    /// - Parameter onCreated: runs after a successful create (refresh the list, select the new topic).
    public init(
        session: ClusterSession,
        existingNames: @escaping @MainActor () -> Set<String> = { [] },
        onCreated: @escaping @MainActor (String) async -> Void = { _ in }
    ) {
        self.session = session
        self.existingNames = existingNames
        self.onCreated = onCreated
    }

    public var clusterName: String { session.connection.name }
    public var gate: WriteGate { WriteGate(isReadOnly: session.isReadOnly) }
    public var isCreating: Bool { submitState == .creating }

    // MARK: Validation

    /// Every current problem, whether or not the field was touched.
    public var issues: [Issue] {
        var found: [Issue] = []
        if let problem = TopicNameValidator.problem(in: name) {
            found.append(.error(.name, problem.message))
        } else if existingNames().contains(name) {
            found.append(.error(.name, KafkaError.topicAlreadyExists(name).userMessage))
        } else if TopicNameValidator.hasMetricNameCollisionRisk(name) {
            found.append(.warning(.name, String(localized: "Names mixing \".\" and \"_\" can collide in Kafka metrics.")))
        }
        if partitions < 1 {
            found.append(.error(.partitions, String(localized: "Use at least 1 partition.")))
        }
        if replicationFactor < 1 {
            found.append(.error(.replicationFactor, String(localized: "Use a replication factor of at least 1.")))
        }
        if let message = retentionProblem {
            found.append(.error(.retention, message))
        }
        return found
    }

    /// What to show now: only touched fields.
    public var visibleIssues: [Issue] { issues.filter { touched.contains($0.field) } }

    public func issues(for field: Field) -> [Issue] { visibleIssues.filter { $0.field == field } }

    public func touch(_ field: Field) { touched.insert(field) }

    public var isValid: Bool { !issues.contains(where: \.isError) }

    /// Create button state: valid, writable and not already running.
    public var canSubmit: Bool { isValid && gate.allowsWrites && !isCreating }

    private var retentionProblem: String? {
        let text = retentionMs.trimmingCharacters(in: .whitespaces)
        if text.isEmpty { return nil }
        guard let value = Int64(text), value >= -1 else {
            return String(localized: "Retention must be a number of milliseconds, or -1 to keep forever.")
        }
        return nil
    }

    var newTopic: NewTopic {
        var config: [String: String] = [:]
        let retention = retentionMs.trimmingCharacters(in: .whitespaces)
        if !retention.isEmpty { config["retention.ms"] = retention }
        if let cleanupPolicy { config["cleanup.policy"] = cleanupPolicy.rawValue }
        return NewTopic(name: name, partitions: partitions, replicationFactor: replicationFactor, config: config)
    }

    // MARK: Submit

    /// Starts the create and returns at once. The work is owned by this model, not by the view, so closing the sheet
    /// does not cancel a request the broker may already have.
    public func submit() {
        guard submitTask == nil else { return }
        submitTask = Task { [weak self] in
            await self?.create()
            self?.submitTask = nil
        }
    }

    /// Validates, then creates. A second call while one is running does nothing.
    public func create() async {
        touched = [.name, .partitions, .replicationFactor, .retention]
        guard !isCreating, isValid else { return }
        guard gate.allowsWrites else {
            submitState = .failed(message: KafkaError.readOnly(clusterName: clusterName).userMessage)
            return
        }
        submitState = .creating
        let spec = newTopic
        do {
            let client = try await session.ensureConnected()
            try await client.createTopic(spec)
        } catch {
            submitState = .failed(message: Self.message(for: error))
            return
        }
        submitState = .created(name: spec.name)
        await onCreated(spec.name)
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case let error as KafkaError: error.userMessage
        case let error as ClusterSessionError: error.userMessage
        default: String(localized: "Couldn't create the topic.")
        }
    }
}
