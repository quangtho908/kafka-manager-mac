// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "KafkaManager",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "KafkaManagerCore", targets: ["KafkaManagerCore"]),
        .executable(name: "KafkaManager", targets: ["KafkaManagerApp"]),
    ],
    targets: [
        // UI-free domain + Kafka client layer. No SwiftUI/AppKit here.
        .target(name: "KafkaManagerCore"),
        .executableTarget(name: "KafkaManagerApp", dependencies: ["KafkaManagerCore"]),
        .testTarget(name: "KafkaManagerCoreTests", dependencies: ["KafkaManagerCore"]),
    ]
)
