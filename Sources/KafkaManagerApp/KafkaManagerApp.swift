import SwiftUI
import KafkaManagerCore

@main
struct KafkaManagerApp: App {
    var body: some Scene {
        WindowGroup("Kafka Manager") {
            ContentView()
        }
    }
}

struct ContentView: View {
    var body: some View {
        NavigationSplitView {
            List { Text("Clusters") }
        } detail: {
            Text("Select a cluster").foregroundStyle(.secondary)
        }
        .frame(minWidth: 800, minHeight: 500)
    }
}
