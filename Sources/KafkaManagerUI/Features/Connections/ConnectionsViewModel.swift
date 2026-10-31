import Foundation
import Observation
import KafkaManagerCore

/// The saved-connections list. Nothing happens at init; the first `load()` is the single lazy read.
@MainActor @Observable
public final class ConnectionsViewModel {
    public private(set) var state = LoadState<[ClusterConnection]>.idle
    public var selection: UUID?
    /// Where an unreadable database was moved to, until the user dismisses the notice.
    public private(set) var recoveredBackup: URL?
    /// Message for the last failed delete, until dismissed or the next delete.
    public private(set) var deleteError: String?

    @ObservationIgnored private let store: any ConnectionStore
    @ObservationIgnored private let secrets: any SecretStore
    @ObservationIgnored private var checkedRecovery = false

    public init(store: any ConnectionStore, secrets: any SecretStore) {
        self.store = store
        self.secrets = secrets
    }

    /// Loads when idle, or when `refresh` is true (retry after a failure, manual reload). Ignored while loading.
    /// If the calling task is cancelled, the state goes back to what it was and nothing is published.
    public func load(refresh: Bool = false) async {
        guard !state.isLoading, refresh || isIdle else { return }
        let previous = state
        if state.value == nil { state = .loading }

        let connections: [ClusterConnection]
        do {
            connections = try await store.load()
        } catch {
            guard !Task.isCancelled else { state = previous; return }
            state = .failed(message: Self.message(for: error))
            return
        }
        guard !Task.isCancelled else { state = previous; return }
        state = .loaded(connections)

        if !checkedRecovery {
            checkedRecovery = true
            // The store hands the backup out once, so keep it even if cancellation arrives during this call.
            recoveredBackup = await store.takeRecoveryBackup()
        }
    }

    public func dismissRecoveryNotice() {
        recoveredBackup = nil
    }

    public func dismissDeleteError() {
        deleteError = nil
    }

    /// Deletes the password first so a failure never leaves a row whose secret is already gone and a stale Keychain
    /// item behind; if that fails the row stays. Once started, both steps finish and the list reflects the store.
    public func delete(id: UUID) async {
        guard !Task.isCancelled else { return }
        deleteError = nil
        do {
            try await secrets.deletePassword(for: id)
        } catch {
            deleteError = Self.message(for: error)
            return
        }
        do {
            try await store.delete(id: id)
        } catch {
            deleteError = Self.message(for: error)
            return
        }
        if case .loaded(var connections) = state {
            connections.removeAll { $0.id == id }
            state = .loaded(connections)
        }
        if selection == id { selection = nil }
    }

    /// A form wired to the same stores, with the name check excluding the profile being edited.
    public func makeFormViewModel(mode: ConnectionFormViewModel.Mode) -> ConnectionFormViewModel {
        let edited: UUID?
        if case .edit(let connection) = mode { edited = connection.id } else { edited = nil }
        return ConnectionFormViewModel(mode: mode, store: store, secrets: secrets, existingNames: existingNames(excluding: edited))
    }

    public func duplicateDraft(of connection: ClusterConnection) -> ConnectionFormViewModel.Mode {
        .duplicate(connection)
    }

    /// Names of the profiles other than `id`, for the form's uniqueness warning.
    public func existingNames(excluding id: UUID?) -> [String] {
        (state.value ?? []).filter { $0.id != id }.map(\.name)
    }

    /// Reflects a profile the form just saved without re-reading the store. Ignored before the first load, which
    /// will pick the profile up anyway.
    public func didSave(_ connection: ClusterConnection) {
        guard case .loaded(var connections) = state else { return }
        if let index = connections.firstIndex(where: { $0.id == connection.id }) {
            connections[index] = connection
        } else {
            connections.append(connection)
        }
        state = .loaded(connections)
    }

    private var isIdle: Bool {
        if case .idle = state { true } else { false }
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case let error as ConnectionStoreError: error.userMessage
        case let error as SecretStoreError: error.userMessage
        default: String(localized: "The saved connections couldn't be updated.")
        }
    }
}
