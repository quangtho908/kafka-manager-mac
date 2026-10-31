import SwiftUI
import KafkaManagerCore
import KafkaManagerUI

@main
struct KafkaManagerApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @State private var environment = AppEnvironment()
    @State private var formRequest: ConnectionFormRequest?
    @State private var refreshToken = 0

    private enum Metrics {
        static let minWidth: Double = 800
        static let minHeight: Double = 500
        static let defaultWidth: Double = 1000
        static let defaultHeight: Double = 640
    }

    var body: some Scene {
        WindowGroup("Kafka Manager") {
            ShellView(shell: environment.shell, sessions: environment.sessions, formRequest: $formRequest, refreshToken: refreshToken)
            .frame(minWidth: Metrics.minWidth, minHeight: Metrics.minHeight)
        }
        .defaultSize(width: Metrics.defaultWidth, height: Metrics.defaultHeight)
        .windowResizability(.contentMinSize)
        .commands {
            CommandGroup(replacing: .newItem) {
                Button("New Connection") { formRequest = ConnectionFormRequest(mode: .new) }
                    .keyboardShortcut("n", modifiers: .command)
                Button("Refresh") { refreshToken += 1 }
                    .keyboardShortcut("r", modifiers: .command)
            }
        }
    }
}
