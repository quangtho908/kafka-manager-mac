// swift-tools-version:6.2
import PackageDescription
let package = Package(
    name: "skc-check",
    platforms: [.macOS(.v14)],
    dependencies: [.package(url: "https://github.com/swift-server/swift-kafka-client.git", from: "1.0.0")],
    targets: [.executableTarget(name: "check", dependencies: [.product(name: "Kafka", package: "swift-kafka-client")])]
)
