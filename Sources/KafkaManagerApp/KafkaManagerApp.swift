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
        static let sidebarMin: Double = 240
        static let sidebarIdeal: Double = 300
        static let sidebarMax: Double = 420
    }

    var body: some Scene {
        WindowGroup("Kafka Manager") {
            NavigationSplitView {
                ConnectionListView(model: environment.connections, formRequest: $formRequest, refreshToken: refreshToken)
                    .navigationSplitViewColumnWidth(min: Metrics.sidebarMin, ideal: Metrics.sidebarIdeal, max: Metrics.sidebarMax)
            } detail: {
                ContentUnavailableView("Select a connection", systemImage: "externaldrive")
            }
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
