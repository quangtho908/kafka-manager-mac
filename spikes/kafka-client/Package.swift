// swift-tools-version:6.0
// Throwaway spike (T04). Not part of the main package. Links the static librdkafka + OpenSSL from vendor/.
import PackageDescription

let package = Package(
    name: "kafka-client-spike",
    platforms: [.macOS(.v14)],
    targets: [
        .systemLibrary(name: "CRdkafka", path: "Sources/CRdkafka"),
        .executableTarget(
            name: "poc",
            dependencies: ["CRdkafka"],
            linkerSettings: [
                .unsafeFlags([
                    "vendor/install/lib/librdkafka.a",
                    "vendor/install-ssl/lib/libssl.a",
                    "vendor/install-ssl/lib/libcrypto.a",
                ]),
                .linkedLibrary("z"),
            ]
        ),
        // Baseline for measuring what the Kafka library adds to the binary.
        .executableTarget(name: "empty"),
    ],
    swiftLanguageModes: [.v5]
)
