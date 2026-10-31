import SwiftUI

#if DEBUG
private struct SampleStateView: View {
    let state: LoadState<[String]>
    var retry: (() -> Void)? = {}
    var isRefreshing = false

    var body: some View {
        StateView(
            state: state, isEmpty: \.isEmpty, emptyTitle: "No topics", emptyMessage: "Create a topic to get started.",
            emptySymbol: "tray", retry: retry, isRefreshing: isRefreshing,
            emptyActions: { Button("New Topic") {} },
            content: { items in List(items, id: \.self) { Text($0) } })
            .frame(width: 360, height: 260)
    }
}

#Preview("StateView – idle") { SampleStateView(state: .idle) }
#Preview("StateView – loading") { SampleStateView(state: .loading) }
#Preview("StateView – loaded") { SampleStateView(state: .loaded(["orders", "payments", "shipments"])) }
#Preview("StateView – refreshing") { SampleStateView(state: .loaded(["orders", "payments"]), isRefreshing: true) }
#Preview("StateView – empty") { SampleStateView(state: .loaded([])) }
#Preview("StateView – failed") { SampleStateView(state: .failed(message: "Can't reach the cluster at broker:9092.")) }
#Preview("StateView – failed with details") {
    SampleStateView(state: .failed(message: "The cluster reported an error.", details: "Local: Broker transport failure (code 42)"))
}
#Preview("StateView – failed, no retry") { SampleStateView(state: .failed(message: "Not allowed."), retry: nil) }

#Preview("ErrorBanner") {
    VStack(spacing: 12) {
        ErrorBanner(message: "Can't reach the cluster at broker:9092.")
        ErrorBanner(message: "The cluster reported an error.", details: "Local: Broker transport failure (code 42)",
                    onRetry: {}, onDismiss: {})
    }
    .frame(width: 480)
    .padding()
}
#endif
