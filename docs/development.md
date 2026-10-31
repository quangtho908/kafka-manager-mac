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
scripts/dev.sh              build | test | run | deps | itest
Sources/KafkaManagerKafka/  LiveKafkaClient trên librdkafka (chỉ target này thấy CRdKafka)
vendor/                     librdkafka + OpenSSL tĩnh (git-ignored, do scripts/build-deps.sh tạo)
```

## Kafka thật (T60)

- `scripts/dev.sh deps` build `vendor/` một lần (~5 phút, arm64). Không có `vendor/`: `Package.swift` bỏ target `CRdKafka`/`KafkaManagerKafka`/`KafkaManagerKafkaTests`, Core/UI vẫn build và test; app chỉ chạy demo (kết nối thật báo lỗi rõ ràng). `scripts/dev.sh` luôn gọi `swift` với `--manifest-cache none` vì cache manifest của SwiftPM không biết `vendor/` thay đổi; khi chạy `swift` trực tiếp sau khi thêm/xoá `vendor/`, thêm cờ đó một lần.
- App mặc định nói chuyện với broker thật. `KAFKAMANAGER_DEMO=1` chuyển sang cluster demo (Fake); `KAFKAMANAGER_DATABASE=/đường/dẫn.sqlite` dùng DB riêng.
- `scripts/dev.sh itest` chạy test tích hợp với broker `localhost:9092` (đổi bằng `KM_ITEST_BOOTSTRAP`); chỉ tạo/xoá topic `km-itest-*`. `swift test` thường bỏ qua chúng.
- `fetchMessages` mở consumer riêng cho mỗi lần đọc (chỉ `assign`, không subscribe/commit, group.id giả `km-noop-*`), đóng ngay khi xong/huỷ; đọc trên thread riêng, poll lát 100 ms, buffer stream 256 tin (đầy thì dừng poll). `latest(N)` nhiều partition: giữ N tin mới nhất theo timestamp rồi trả theo (partition, offset). `KM_ITEST_READ_TOPIC=<topic>` cho itest đọc (không ghi) một topic có sẵn.
- Nhóm (chỉ đọc): `listGroups` = một `rd_kafka_list_groups` (có member count, lọc `protocol_type` ∈ {consumer, rỗng}); `describeGroup` = DescribeConsumerGroups + ListConsumerGroupOffsets + 2 ListOffsets gộp (earliest/latest cho mọi partition một lần; `describeTopic` cũng dùng ListOffsets gộp). Group broker báo `Dead` không member → `clusterError(code: 69)` (`isUnknownGroup`). Các call admin chạy tuần tự trên executor của actor nên gọi song song an toàn. Itest chỉ join/commit/xoá group `km-itest-g-*` của chính nó.
- Mọi lời gọi C chặn đều chạy trên queue riêng của `LiveKafkaClient`, có timeout (mặc định 10 s); mỗi đối tượng C có wrapper RAII và được đếm trong `ResourceCounters` (test phát hiện rò rỉ).

## Quy ước

- Swift 6 strict concurrency: model là `Sendable`; mọi I/O Kafka đi qua actor, không chạy trên main actor.
- View model: `@Observable` + `@MainActor`, chỉ gọi service trong Core.
- SwiftUI trước; AppKit (`NSViewRepresentable`) chỉ khi SwiftUI không làm được (vd. bảng message rất lớn).
- Test dùng Swift Testing (`import Testing`, `@Test`, `#expect`), không dùng XCTest.
- Mọi gốc của một cột hoặc tab (list, detail, tab trong detail, cột giữa của shell) phải gọi `.topAlignedColumn()` (Shared/TopAlignedColumn.swift); nếu không, nội dung không giãn (empty state, placeholder) bị căn giữa theo chiều dọc. Chỉ placeholder thuần (`ContentUnavailableView`) mới được ở giữa. Kiểm bằng AX: y của phần tử đầu tiên phải sát đỉnh cột ở cửa sổ cao lẫn thấp.
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

Dữ liệu demo hiện có 200 topic. Để thử hiệu năng với cluster lớn: `KAFKAMANAGER_DEMO_TOPICS=5000 swift run KafkaManager` (1–20000; giá trị khác bị bỏ qua; chỉ dùng khi phát triển).

Số message mỗi partition của dữ liệu demo mặc định là 30. Để thử fetch lớn: `KAFKAMANAGER_DEMO_MESSAGES=3400 swift run KafkaManager` (1–5000; giá trị khác bị bỏ qua; chỉ dùng khi phát triển).

## Đóng gói

SwiftPM executable không có entitlements/signing. Trước khi phát hành cần Xcode project (hoặc XcodeGen) với App Sandbox, entitlement `com.apple.security.network.client`, Developer ID signing và notarization.
