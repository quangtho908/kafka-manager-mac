import SwiftUI
import KafkaManagerCore

#if DEBUG
private enum PreviewData {
    static let plain = ClusterConnection(name: "Local dev", bootstrapServers: "localhost:9092")
    static let sasl = ClusterConnection(
        name: "Staging", bootstrapServers: "b1.staging:9093,b2.staging:9093,b3.staging:9093", environment: .staging,
        security: .saslSsl, saslMechanism: .scramSha512, username: "alice")
    static let production = ClusterConnection(
        name: "Production EU", bootstrapServers: "kafka-eu-1:9092,kafka-eu-2:9092", environment: .production, security: .ssl)

    @MainActor
    static func list(_ connections: [ClusterConnection], failing: Bool = false) -> ConnectionsViewModel {
        ConnectionsViewModel(store: PreviewStore(connections: connections, failing: failing), secrets: InMemorySecretStore())
    }
}

private struct PreviewStore: ConnectionStore {
    let connections: [ClusterConnection]
    let failing: Bool

    func load() async throws -> [ClusterConnection] {
        if failing { throw ConnectionStoreError.database(code: 1, message: "") }
        return connections
    }
    func save(_ connection: ClusterConnection) async throws {}
    func delete(id: UUID) async throws {}
}

private struct ListPreview: View {
    @State var model: ConnectionsViewModel
    @State private var request: ConnectionFormRequest?

    var body: some View {
        ConnectionListView(model: model, formRequest: $request)
            .frame(width: 300, height: 360)
    }
}

#Preview("List – connections") {
    ListPreview(model: PreviewData.list([PreviewData.plain, PreviewData.sasl, PreviewData.production]))
}

#Preview("List – empty") {
    ListPreview(model: PreviewData.list([]))
}

#Preview("List – failed") {
    ListPreview(model: PreviewData.list([], failing: true))
}

#Preview("Form – new") {
    ConnectionFormView(model: ConnectionFormViewModel(mode: .new, store: InMemoryConnectionStore(), secrets: InMemorySecretStore(), existingNames: []), onSaved: { _ in })
}

#Preview("Form – edit SASL") {
    ConnectionFormView(
        model: ConnectionFormViewModel(mode: .edit(PreviewData.sasl), store: InMemoryConnectionStore(), secrets: InMemorySecretStore(), existingNames: []),
        onSaved: { _ in })
}

#Preview("Form – edit Production") {
    ConnectionFormView(
        model: ConnectionFormViewModel(mode: .edit(PreviewData.production), store: InMemoryConnectionStore(), secrets: InMemorySecretStore(), existingNames: ["Production EU"]),
        onSaved: { _ in })
}
#endif
