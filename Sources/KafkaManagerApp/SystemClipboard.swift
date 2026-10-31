import AppKit
import KafkaManagerUI

/// The one place the app touches `NSPasteboard`; the UI module only knows the `Clipboard` protocol.
@MainActor
final class SystemClipboard: Clipboard {
    func copy(_ text: String) {
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(text, forType: .string)
    }
}
