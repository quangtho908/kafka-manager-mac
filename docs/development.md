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

**macOS 14 (Sonoma) là mức tối thiểu** (`Package.swift`: `.macOS(.v14)`), chốt 2026-10-31 để hỗ trợ máy cũ. Build bằng toolchain mới nhất nhưng không được vô tình phụ thuộc API mới hơn.

Skill `swiftui-macos` nhắm macOS 26 / Swift 6.2, nên khi áp dụng skill:
- API chỉ có từ macOS 15/26 (ví dụ `Observations {}`, `UtilityWindow`, một số kiểu window/toolbar/`Table` mới) **không dùng trực tiếp**. Nếu thật sự cần: bọc `if #available(macOS NN, *)` / `@available` **và** có đường dự phòng chạy được trên macOS 14.
- `@Observable`, `NavigationSplitView`, `Table`, Swift Testing, `MenuBarExtra` dùng được trên 14.
- Vì SDK mới không báo lỗi khi gọi API mới mà thiếu guard trên target 14 chỉ ở một số trường hợp, coder phải kiểm tra từng API lạ trong tài liệu (mục *Availability*), không dựa vào việc build qua.
- Nếu skill khuyên một pattern của 26 mà 14 không có, ghi rõ trong báo cáo mục "Ngoại lệ có chủ ý" cách đã thay thế.

## Chạy với database tạm

Để không đụng profile thật: `KAFKAMANAGER_DATABASE=/tmp/x.sqlite swift run KafkaManager` (chỉ dùng khi phát triển/test, không dành cho người dùng).

## Đóng gói

SwiftPM executable không có entitlements/signing. Trước khi phát hành cần Xcode project (hoặc XcodeGen) với App Sandbox, entitlement `com.apple.security.network.client`, Developer ID signing và notarization.
