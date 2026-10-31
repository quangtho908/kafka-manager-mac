import SwiftUI

/// Where "Copy" puts text. The app target supplies the system pasteboard; tests supply a fake.
@MainActor
public protocol Clipboard: AnyObject {
    func copy(_ text: String)
}

/// Used when nothing is injected (previews): copying does nothing.
@MainActor
final class NoClipboard: Clipboard {
    nonisolated init() {}
    func copy(_ text: String) {}
}

private struct ClipboardKey: EnvironmentKey {
    static var defaultValue: any Clipboard { NoClipboard() }
}

extension EnvironmentValues {
    public var clipboard: any Clipboard {
        get { self[ClipboardKey.self] }
        set { self[ClipboardKey.self] = newValue }
    }
}
