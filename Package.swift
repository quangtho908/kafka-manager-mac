// swift-tools-version:6.0
import Foundation
import PackageDescription

// The live Kafka client needs the static librdkafka + OpenSSL from `vendor/` (built by `scripts/build-deps.sh`).
// Without vendor/ the Kafka targets are left out, so Core, UI and their tests still build and run, and the app is
// demo-only. `scripts/dev.sh deps` builds vendor/.
let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().path
let hasVendor = FileManager.default.fileExists(atPath: "\(root)/vendor/install/lib/librdkafka.a")
    && FileManager.default.fileExists(atPath: "\(root)/vendor/install-ssl/lib/libssl.a")

var targets: [Target] = [
    // UI-free domain + Kafka client layer. No SwiftUI/AppKit here.
    .target(name: "KafkaManagerCore"),
    // View models and shared UI state (Observation + Foundation only; SwiftUI views arrive with T13b).
    .target(name: "KafkaManagerUI", dependencies: ["KafkaManagerCore"]),
    .executableTarget(
        name: "KafkaManagerApp",
        dependencies: ["KafkaManagerUI", "KafkaManagerCore"] + (hasVendor ? ["KafkaManagerKafka"] : []),
        swiftSettings: hasVendor ? [.define("LIVE_KAFKA")] : []),
    .testTarget(name: "KafkaManagerCoreTests", dependencies: ["KafkaManagerCore"]),
    .testTarget(name: "KafkaManagerUITests", dependencies: ["KafkaManagerUI", "KafkaManagerCore"]),
]

if hasVendor {
    targets += [
        // Headers of the vendored librdkafka. Only KafkaManagerKafka may depend on it.
        .systemLibrary(name: "CRdKafka", path: "Sources/CRdKafka"),
        .target(
            name: "KafkaManagerKafka",
            dependencies: ["KafkaManagerCore", "CRdKafka"],
            linkerSettings: [
                .unsafeFlags([
                    "\(root)/vendor/install/lib/librdkafka.a",
                    "\(root)/vendor/install-ssl/lib/libssl.a",
                    "\(root)/vendor/install-ssl/lib/libcrypto.a",
                ]),
                .linkedLibrary("z"),
            ]),
        // Unit tests need no broker; the integration tests inside are skipped unless KM_ITEST=1 (scripts/dev.sh itest).
        // KafkaManagerUI lets them drive a ClusterSession and ClientConnectionTester on the live client.
        .testTarget(name: "KafkaManagerKafkaTests", dependencies: ["KafkaManagerKafka", "KafkaManagerUI", "KafkaManagerCore", "CRdKafka"]),
    ]
}

let package = Package(
    name: "KafkaManager",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "KafkaManagerCore", targets: ["KafkaManagerCore"]),
        .executable(name: "KafkaManager", targets: ["KafkaManagerApp"]),
    ],
    targets: targets
)
