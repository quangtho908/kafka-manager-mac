import SwiftUI

/// Inline error strip for a screen that is still usable: icon + message (never colour alone), optional Retry and
/// Dismiss, and collapsed copyable details. `message` comes from a view model's user-facing text.
public struct ErrorBanner: View {
    let message: String
    let details: String?
    let onRetry: (() -> Void)?
    let onDismiss: (() -> Void)?

    public init(message: String, details: String? = nil, onRetry: (() -> Void)? = nil, onDismiss: (() -> Void)? = nil) {
        self.message = message
        self.details = details
        self.onRetry = onRetry
        self.onDismiss = onDismiss
    }

    private enum Metrics {
        static let padding: Double = 10
        static let spacing: Double = 8
        static let tint = 0.15
    }

    public var body: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            HStack(alignment: .firstTextBaseline, spacing: Metrics.spacing) {
                Label {
                    Text(message)
                } icon: {
                    Image(systemName: "xmark.octagon.fill").foregroundStyle(.red)
                }
                .accessibilityElement(children: .combine)
                .accessibilityLabel(Text("Error: \(message)"))
                Spacer()
                if let onRetry { Button("Retry", action: onRetry) }
                if let onDismiss { Button("Dismiss", action: onDismiss) }
            }
            if let details { ErrorDetails(details: details) }
        }
        .padding(Metrics.padding)
        .background(Color.red.opacity(Metrics.tint))
        .accessibilityElement(children: .contain)
    }
}
