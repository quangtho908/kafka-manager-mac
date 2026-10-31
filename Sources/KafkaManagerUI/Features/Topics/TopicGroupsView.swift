import SwiftUI
import KafkaManagerCore

/// The "Consumer Groups" tab of a topic: groups that consume it, found by describing the groups a few at a time.
struct TopicGroupsView: View {
    let model: TopicGroupsViewModel
    let openGroup: (String) -> Void

    @State private var selection: String?
    @State private var scanCount = 0

    private enum Metrics {
        static let padding: Double = 12
        static let spacing: Double = 8
        static let stateMin: Double = 150
        static let lagMin: Double = 150
    }

    var body: some View {
        VStack(spacing: 0) {
            status
            content
        }
        // Starts when the tab opens, stops when it closes or the topic changes.
        .task(id: scanCount) { await model.scanGroups() }
        .onChange(of: selection) { _, id in
            if let id { openGroup(id) }
        }
    }

    private var status: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing / 2) {
            HStack(spacing: Metrics.spacing) {
                if model.isScanning { ProgressView().controlSize(.small).accessibilityLabel(Text("Scanning")) }
                Text(model.progressText).font(.callout).foregroundStyle(.secondary).monospacedDigit()
                Spacer()
                Button {
                    scanCount += 1
                } label: {
                    Label("Scan again", systemImage: "arrow.clockwise")
                }
                .disabled(model.isScanning)
            }
            Label("Kafka can't list a topic's consumers directly, so each group is checked on the cluster.", systemImage: "info.circle")
                .font(.caption)
                .foregroundStyle(.secondary)
            if model.unreadableCount > 0 {
                Label("\(model.unreadableCount) groups couldn't be read.", systemImage: "exclamationmark.triangle")
                    .font(.caption)
                    .foregroundStyle(.orange)
            }
        }
        .padding(Metrics.padding)
    }

    @ViewBuilder private var content: some View {
        if case .failed(let message) = model.scan {
            ErrorBanner(message: message, onRetry: { scanCount += 1 })
        } else if model.rows.isEmpty {
            if case .finished = model.scan {
                ContentUnavailableView(
                    "No consumer groups", systemImage: "person.3",
                    description: Text("No group has committed offsets for, or is assigned, this topic."))
            } else {
                Spacer()
            }
        } else {
            Table(model.rows, selection: $selection) {
                TableColumn("Group ID") { Text($0.groupId).lineLimit(1).truncationMode(.middle) }
                TableColumn("State") { GroupStateLabel(state: $0.state) }
                    .width(min: Metrics.stateMin)
                TableColumn("Lag on this topic") { row in
                    let severity = LagSeverityPresentation(row.severity)
                    Label {
                        Text(GroupLag.shortText(row.lag)).monospacedDigit()
                    } icon: {
                        Image(systemName: severity.symbolName)
                    }
                    .foregroundStyle(LagTint.color(severity.tone))
                    .help(Text("\(GroupLag.exactText(row.lag)) messages · \(severity.label)"))
                }
                .width(min: Metrics.lagMin)
            }
            .tableStyle(.inset(alternatesRowBackgrounds: true))
        }
    }
}
