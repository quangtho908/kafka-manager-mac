/// Which part of a `StateView` a `LoadState` shows. Pure, so the choice is unit-tested without views.
public enum StateBranch: Equatable, Sendable {
    case idle
    case loading
    case failed(message: String, details: String?)
    case empty
    case content

    public static func resolve<Value: Sendable>(_ state: LoadState<Value>, isEmpty: (Value) -> Bool) -> StateBranch {
        switch state {
        case .idle: .idle
        case .loading: .loading
        case .failed(let message, let details): .failed(message: message, details: details)
        case .loaded(let value): isEmpty(value) ? .empty : .content
        }
    }
}
