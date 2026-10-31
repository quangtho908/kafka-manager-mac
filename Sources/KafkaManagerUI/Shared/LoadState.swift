/// Lifecycle of data a screen loads on demand.
public enum LoadState<Value: Sendable>: Sendable {
    case idle
    case loading
    case loaded(Value)
    case failed(message: String)

    public var value: Value? {
        if case .loaded(let value) = self { value } else { nil }
    }

    public var isLoading: Bool {
        if case .loading = self { true } else { false }
    }

    public var failureMessage: String? {
        if case .failed(let message) = self { message } else { nil }
    }
}

extension LoadState: Equatable where Value: Equatable {}
