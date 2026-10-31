# Development

## Setup

```sh
git clone <repo> && cd kafka-manager
scripts/dev.sh build
scripts/dev.sh test
```

Toolchain: Swift 6 (đã kiểm tra với Swift 6.4, Xcode 27). Mở `Package.swift` bằng Xcode để debug UI.

## Cấu trúc

```
Sources/KafkaManagerCore/   domain models, Kafka client layer (không import UI)
Sources/KafkaManagerApp/    SwiftUI app (@main), chỉ phụ thuộc Core
Tests/KafkaManagerCoreTests Swift Testing
scripts/dev.sh              build | test | run
```

## Quy ước

- Swift 6 strict concurrency: model là `Sendable`; mọi I/O Kafka đi qua actor, không chạy trên main actor.
- View model: `@Observable` + `@MainActor`, chỉ gọi service trong Core.
- SwiftUI trước; AppKit (`NSViewRepresentable`) chỉ khi SwiftUI không làm được (vd. bảng message rất lớn).
- Test dùng Swift Testing (`import Testing`, `@Test`, `#expect`), không dùng XCTest.
- Hướng dẫn SwiftUI chi tiết: skill `swiftui-macos` trong `.claude/skills/`.

## Kafka local để thử

Chạy broker đơn lẻ bằng Docker (chưa có sẵn trong repo; dự kiến đặt ở `tools/` của workspace):

```sh
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```

Bootstrap server: `localhost:9092`.

## Deployment target

`Package.swift` hiện đặt `.macOS(.v14)`. Skill `swiftui-macos` nhắm macOS 26+; nếu không cần hỗ trợ bản cũ, nâng lên `.macOS(.v26)` để dùng API mới.

## Đóng gói

SwiftPM executable không có entitlements/signing. Trước khi phát hành cần Xcode project (hoặc XcodeGen) với App Sandbox, entitlement `com.apple.security.network.client`, Developer ID signing và notarization.
