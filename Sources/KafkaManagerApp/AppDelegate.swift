import AppKit

/// An unbundled run (`swift run`, the raw binary) has no Info.plist, so macOS starts it as a background-only
/// process: no Dock icon, no menu bar, no key window. Asking for the regular policy and activating makes it behave
/// like a normal app. Inside a real .app bundle the policy is already `.regular`, so both calls change nothing.
final class AppDelegate: NSObject, NSApplicationDelegate {
    func applicationWillFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        if let selfTest = SelfTest.requested {
            // A packaged-build check, not a session: run it, report, and leave without showing anything.
            NSApp.windows.forEach { $0.close() }
            Task { exit(await SelfTest.run(selfTest)) }
            return
        }
        // The plain `activate()` (macOS 14) only yields focus when the frontmost app cooperates, so a launch from a
        // terminal or browser stays behind it; this form brings the window up. Still available on macOS 14.
        NSApp.activate(ignoringOtherApps: true)
    }
}
