import SwiftUI

/// One consistent way to show idle / loading / failed / empty / loaded. The failed branch shows the view model's
/// user-facing message with Retry and a collapsed "Details"; the view never formats raw errors itself.
public struct StateView<Value: Sendable, Content: View, EmptyActions: View>: View {
    let state: LoadState<Value>
    let isEmpty: (Value) -> Bool
    let loadingTitle: String
    let failureTitle: String
    let emptyTitle: String
    let emptyMessage: String
    let emptySymbol: String
    let retry: (() -> Void)?
    /// Shows a small spinner over the content while a refresh runs; the content stays visible.
    let isRefreshing: Bool
    let emptyActions: EmptyActions
    let content: (Value) -> Content

    public init(
        state: LoadState<Value>,
        isEmpty: @escaping (Value) -> Bool,
        loadingTitle: String = String(localized: "Loading…"),
        failureTitle: String = String(localized: "Couldn't load"),
        emptyTitle: String,
        emptyMessage: String = "",
        emptySymbol: String,
        retry: (() -> Void)? = nil,
        isRefreshing: Bool = false,
        @ViewBuilder emptyActions: () -> EmptyActions,
        @ViewBuilder content: @escaping (Value) -> Content
    ) {
        self.state = state
        self.isEmpty = isEmpty
        self.loadingTitle = loadingTitle
        self.failureTitle = failureTitle
        self.emptyTitle = emptyTitle
        self.emptyMessage = emptyMessage
        self.emptySymbol = emptySymbol
        self.retry = retry
        self.isRefreshing = isRefreshing
        self.emptyActions = emptyActions()
        self.content = content
    }

    public var body: some View {
        switch StateBranch.resolve(state, isEmpty: isEmpty) {
        case .idle:
            Color.clear
        case .loading:
            ProgressView(loadingTitle)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        case .failed(let message, let details):
            ContentUnavailableView {
                Label(failureTitle, systemImage: "exclamationmark.triangle")
            } description: {
                VStack(spacing: Metrics.spacing) {
                    Text(message)
                    if let details { ErrorDetails(details: details) }
                }
            } actions: {
                if let retry { Button("Retry", action: retry) }
            }
        case .empty:
            ContentUnavailableView {
                Label(emptyTitle, systemImage: emptySymbol)
            } description: {
                if !emptyMessage.isEmpty { Text(emptyMessage) }
            } actions: {
                emptyActions
            }
        case .content:
            if let value = state.value {
                content(value)
                    .overlay(alignment: .topTrailing) {
                        if isRefreshing {
                            ProgressView()
                                .controlSize(.small)
                                .padding(Metrics.spacing)
                                .accessibilityLabel(Text("Refreshing"))
                        }
                    }
            }
        }
    }

}

private enum Metrics {
    static let spacing: Double = 8
}

extension StateView where EmptyActions == EmptyView {
    public init(
        state: LoadState<Value>,
        isEmpty: @escaping (Value) -> Bool,
        loadingTitle: String = String(localized: "Loading…"),
        failureTitle: String = String(localized: "Couldn't load"),
        emptyTitle: String,
        emptyMessage: String = "",
        emptySymbol: String,
        retry: (() -> Void)? = nil,
        isRefreshing: Bool = false,
        @ViewBuilder content: @escaping (Value) -> Content
    ) {
        self.init(
            state: state, isEmpty: isEmpty, loadingTitle: loadingTitle, failureTitle: failureTitle,
            emptyTitle: emptyTitle, emptyMessage: emptyMessage, emptySymbol: emptySymbol, retry: retry,
            isRefreshing: isRefreshing, emptyActions: { EmptyView() }, content: content)
    }
}

/// Collapsed "Details" with selectable, copyable technical text.
struct ErrorDetails: View {
    let details: String

    var body: some View {
        DisclosureGroup("Details") {
            Text(details)
                .font(.caption.monospaced())
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}
