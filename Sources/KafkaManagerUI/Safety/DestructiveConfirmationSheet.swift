import SwiftUI
import KafkaManagerCore

/// Modal confirmation for a destructive action. Cancel is the default button, so Return alone never confirms; the
/// red action stays disabled until the exact name is typed.
public struct DestructiveConfirmationSheet: View {
    let confirmation: DestructiveConfirmation
    let gate: WriteGate
    let onConfirm: () -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var typed = ""
    @FocusState private var fieldFocused: Bool

    private enum Metrics {
        static let spacing: Double = 12
        static let padding: Double = 20
        static let width: Double = 420
        static let bannerPadding: Double = 8
        static let corner: Double = 6
        static let tint: Double = 0.18
    }

    public init(confirmation: DestructiveConfirmation, gate: WriteGate = WriteGate(isReadOnly: false), onConfirm: @escaping () -> Void) {
        self.confirmation = confirmation
        self.gate = gate
        self.onConfirm = onConfirm
    }

    private var canConfirm: Bool { gate.allowsWrites && confirmation.isSatisfied(by: typed) }

    public var body: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            environmentBanner
            Text(confirmation.title).font(.headline)
            Text(confirmation.consequences).fixedSize(horizontal: false, vertical: true)
            if let reason = gate.disabledReason {
                Label(reason, systemImage: "lock.fill").foregroundStyle(.secondary)
            } else {
                VStack(alignment: .leading, spacing: 4) {
                    Text(confirmation.fieldLabel).font(.subheadline)
                    Text(confirmation.objectName).font(.body.monospaced()).textSelection(.enabled)
                    TextField(confirmation.objectKind, text: $typed)
                        .labelsHidden()
                        .textFieldStyle(.roundedBorder)
                        .focused($fieldFocused)
                        .autocorrectionDisabled()
                        .accessibilityLabel(confirmation.fieldLabel)
                        .onSubmit { if canConfirm { confirm() } }
                }
            }
            HStack {
                Spacer()
                Button("Cancel", role: .cancel) { dismiss() }
                    .keyboardShortcut(.defaultAction)
                if gate.allowsWrites {
                    Button(confirmation.actionTitle, role: .destructive) { confirm() }
                        .disabled(!canConfirm)
                }
            }
        }
        .padding(Metrics.padding)
        .frame(width: Metrics.width)
        .accessibilityElement(children: .contain)
        .onAppear { fieldFocused = true }
    }

    private func confirm() {
        onConfirm()
        dismiss()
    }

    @ViewBuilder private var environmentBanner: some View {
        let environment = confirmation.environment
        let text = "\(environment.label) · \(confirmation.clusterName)"
        if confirmation.showsProductionBanner {
            Label(text, systemImage: environment.symbolName)
                .font(.headline)
                .foregroundStyle(.white)
                .padding(Metrics.bannerPadding)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(Color.red.opacity(0.9), in: RoundedRectangle(cornerRadius: Metrics.corner))
                .accessibilityLabel(confirmation.accessibilitySummary)
        } else {
            Label(text, systemImage: environment.symbolName)
                .font(.subheadline.weight(.semibold))
                .padding(Metrics.bannerPadding)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(Color.secondary.opacity(Metrics.tint), in: RoundedRectangle(cornerRadius: Metrics.corner))
                .accessibilityLabel(confirmation.accessibilitySummary)
        }
    }
}

private func sample(_ environment: ClusterConnection.Environment) -> DestructiveConfirmation {
    DestructiveConfirmation(
        title: String(localized: "Delete topic?"), clusterName: "prod-eu", environment: environment,
        objectKind: String(localized: "topic"), objectName: "orders.v1", actionTitle: String(localized: "Delete Topic"),
        consequences: String(localized: "All messages in this topic are deleted permanently. This cannot be undone."))
}

#Preview("Local") { DestructiveConfirmationSheet(confirmation: sample(.local)) {} }
#Preview("Staging") { DestructiveConfirmationSheet(confirmation: sample(.staging)) {} }
#Preview("Production") { DestructiveConfirmationSheet(confirmation: sample(.production)) {} }
#Preview("Read-only") {
    DestructiveConfirmationSheet(confirmation: sample(.production), gate: WriteGate(isReadOnly: true)) {}
}
