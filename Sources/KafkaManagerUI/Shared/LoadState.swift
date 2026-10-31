/// Lifecycle of data a screen loads on demand.
public enum LoadState<Value: Sendable>: Sendable {
    case idle
    case loading
    case loaded(Value)
    /// `message` is the user-facing text (a `userMessage`); `details` is optional technical context that is safe to
    /// show and copy (never a secret, never raw library output).
    case failed(message: String, details: String? = nil)

    public var value: Value? {
        if case .loaded(let value) = self { value } else { nil }
    }

    public var isLoading: Bool {
        if case .loading = self { true } else { false }
    }

    public var failureMessage: String? {
        if case .failed(let message, _) = self { message } else { nil }
    }

    public var failureDetails: String? {
        if case .failed(_, let details) = self { details } else { nil }
    }
}

extension LoadState: Equatable where Value: Equatable {}
