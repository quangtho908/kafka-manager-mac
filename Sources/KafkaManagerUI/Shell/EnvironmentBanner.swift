import SwiftUI

/// Thin strip with the cluster, its environment and the read-only switch.
struct EnvironmentBanner: View {
    let presentation: EnvironmentBannerPresentation
    /// nil while the session is still being resolved.
    let onSetReadOnly: ((Bool) -> Void)?

    @State private var confirming = false

    private enum Metrics {
        static let spacing: Double = 8
        static let horizontal: Double = 10
        static let vertical: Double = 6
        static let tint: Double = 0.18
    }

    var body: some View {
        HStack(spacing: Metrics.spacing) {
            VStack(alignment: .leading, spacing: 2) {
                Label(presentation.title, systemImage: presentation.environmentSymbol)
                    .lineLimit(1)
                    .truncationMode(.tail)
                    .font(presentation.isEmphasized ? .headline : .subheadline.weight(.semibold))
                Label(presentation.readOnlyLabel, systemImage: presentation.readOnlySymbol)
                    .font(.caption)
            }
            .accessibilityElement(children: .ignore)
            .accessibilityLabel(presentation.accessibilityLabel)
            Spacer(minLength: Metrics.spacing)
            Button(presentation.toggleTitle) {
                if presentation.toggleNeedsConfirmation {
                    confirming = true
                } else {
                    onSetReadOnly?(!presentation.isReadOnly)
                }
            }
            .controlSize(.small)
            .disabled(onSetReadOnly == nil)
        }
        .padding(.horizontal, Metrics.horizontal)
        .padding(.vertical, Metrics.vertical)
        .foregroundStyle(foreground)
        .background(background)
        .confirmationDialog(presentation.confirmationTitle, isPresented: $confirming, titleVisibility: .visible) {
            Button("Cancel", role: .cancel) {}
            Button(presentation.confirmationButtonTitle, role: .destructive) { onSetReadOnly?(false) }
        } message: {
            Text(presentation.confirmationMessage)
        }
    }

    private var color: Color {
        switch presentation.tone {
        case .calm: .green
        case .caution: .orange
        case .danger: .red
        }
    }

    private var foreground: Color {
        presentation.isEmphasized ? .white : .primary
    }

    private var background: Color {
        presentation.isEmphasized ? color.opacity(0.9) : color.opacity(Metrics.tint)
    }
}

#Preview("Local") {
    EnvironmentBanner(presentation: .init(clusterName: "dev", environment: .local, isReadOnly: false), onSetReadOnly: { _ in })
        .frame(width: 280)
}

#Preview("Production, read-only") {
    EnvironmentBanner(presentation: .init(clusterName: "prod-eu", environment: .production, isReadOnly: true), onSetReadOnly: { _ in })
        .frame(width: 280)
}
