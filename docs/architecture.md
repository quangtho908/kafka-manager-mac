# Architecture

Mục tiêu: app macOS native quản lý Kafka (cluster, topic, message, consumer group). Tính năng: [product.md](product.md) · spec: [features/](features/README.md).

## 1. Nguyên tắc

1. **UI không biết Kafka.** View chỉ đọc state của view model; view model chỉ gọi protocol `KafkaClient`.
2. **Không phụ thuộc thư viện Kafka cụ thể.** Mọi tính năng viết trên protocol; client thật chỉ là một implementation. Chưa chốt client vẫn làm được UI với `FakeKafkaClient`.
3. **I/O ngoài main actor.** Mọi truy cập client đi qua `actor`; model trả về là `Sendable`.
4. **An toàn ở tầng thấp.** Chặn thao tác ghi trên cluster read-only ở Core, không chỉ ẩn nút.
5. **Secret không rời Keychain.**

## 2. Các tầng

```
┌──────────────────────────────────────────────┐
│ SwiftUI Views                    (App)       │
├──────────────────────────────────────────────┤
│ View models  @Observable @MainActor  (App)   │
├──────────────────────────────────────────────┤
│ Services     actor, protocol        (Core)   │
│  KafkaClient · ConnectionStore · SecretStore │
├──────────────────────────────────────────────┤
│ Adapters                                     │
│  LiveKafkaClient (librdkafka) · Fake · Guard │
└──────────────────────────────────────────────┘
```

| Tầng | Trách nhiệm | Không được làm |
|---|---|---|
| View | Hiển thị, nhận input | Gọi Kafka, giữ logic nghiệp vụ |
| View model | State UI, gọi service, map lỗi → thông báo, huỷ Task | Import Kafka library |
| Service (protocol) | Hợp đồng nghiệp vụ, model `Sendable` | Import SwiftUI/AppKit |
| Adapter | Nói chuyện với broker/Keychain/đĩa | Rò kiểu của thư viện ngoài ra ngoài |

## 3. Module (SwiftPM)

| Target | Phụ thuộc | Nội dung |
|---|---|---|
| `KafkaManagerCore` | — | Model, protocol, `Fake`, `Guarded`, store, `KafkaError` |
| `KafkaManagerKafka` *(thêm sau khi chốt client)* | Core, thư viện Kafka | `LiveKafkaClient` |
| `KafkaManagerApp` | Core (+ Kafka ở composition root) | View, view model, entry point |

Tách `KafkaManagerKafka` để thư viện C/ngoài chỉ nằm một chỗ: đổi client chỉ sửa target này; Core và test không bị kéo theo thời gian build/link của C.

Cấu trúc thư mục dự kiến:

```
Sources/KafkaManagerCore/
  Models/        ClusterConnection, Topic*, KafkaMessage, Group*, ...
  Client/        KafkaClient, KafkaError, GuardedKafkaClient, FakeKafkaClient
  Storage/       ConnectionStore, SecretStore (Keychain + in-memory)
Sources/KafkaManagerApp/
  App/           KafkaManagerApp, AppEnvironment (composition root)
  Features/      Connections/ Topics/ Messages/ Groups/  (View + ViewModel mỗi nhóm)
  Shared/        StateView, ErrorBanner, extension
```

## 4. Luồng dữ liệu

```
User action ─► View ─► ViewModel (@MainActor)
                         │  Task { try await client.listTopics() }
                         ▼
                   GuardedKafkaClient  ── read-only? ─► throw
                         ▼
                   LiveKafkaClient (actor) ─► broker
                         │
        Sendable model / KafkaError
                         ▼
                   ViewModel cập nhật state ─► View render
```

- Một `ClusterSession` cho mỗi cluster đang mở: giữ `KafkaClient` (đã bọc `Guarded`), trạng thái kết nối (`connecting / connected / failed(KafkaError)`), và các view model con của cluster đó.
- `AppEnvironment` (composition root) tạo `ConnectionStore`, `SecretStore`, và một factory `(ClusterConnection) -> KafkaClient`. Test/preview truyền factory trả `FakeKafkaClient`.

## 5. Concurrency

- Swift 6 strict concurrency. Model `Sendable`; không chia sẻ trạng thái mutable ngoài actor.
- `LiveKafkaClient` là `actor`: client C không thread-safe, actor tuần tự hoá truy cập. Lời gọi chặn (poll librdkafka) chạy trong Task/thread riêng, không chiếm cooperative pool lâu — chi tiết khi chọn client.
- View model huỷ Task khi view biến mất hoặc người dùng bấm Stop; `onTermination` của `AsyncThrowingStream` dừng consumer.
- Mỗi lời gọi có timeout rõ ràng; không có await vô hạn.
- Xem `.claude/skills/swiftui-macos` (concurrency, observation) khi viết view model.

## 6. Quản lý state

- `@Observable` view model, sở hữu bằng `@State` ở view gốc của tính năng; truyền xuống bằng tham số hoặc `.environment`.
- State dạng enum cho mọi màn tải dữ liệu: `idle | loading | loaded(T) | failed(KafkaError)`; `StateView` dùng chung hiển thị.
- Lọc/sắp xếp là hàm thuần trên dữ liệu đã tải → dễ test, không gọi lại broker.
- Danh sách lớn (topic, message): dùng `Table`/`List` ảo hoá; cập nhật theo batch (~100 ms) cho stream message.

## 7. Streaming message

`fetchMessages(query) -> AsyncThrowingStream<KafkaMessage, Error>`:

- Consumer riêng, **không commit offset, không dùng group.id của người dùng** → không làm lệch group thật.
- Dừng khi đủ `limit`, hết dữ liệu, hoặc Task bị huỷ.
- Buffer có giới hạn (`bufferingPolicy: .bufferingNewest/…` tuỳ chế độ); view model gom batch rồi mới ghi vào state.
- Value lớn: bảng giữ bản rút gọn, bản đầy đủ tải khi chọn dòng.

## 8. Lỗi

- Mọi lỗi từ adapter được map sang `KafkaError` (một enum, có `userMessage`); view model không bao giờ hiện lỗi thô của thư viện.
- Giữ `code` + message gốc trong `clusterError` để phần "Details" cho người dùng copy khi báo lỗi.
- Lỗi một phần (vd. không có quyền describe một topic) không làm hỏng cả danh sách.

## 9. Lưu trữ và bảo mật

| Dữ liệu | Nơi lưu |
|---|---|
| Profile cluster (không secret) | `~/Library/Application Support/KafkaManager/connections.json`, ghi atomic, giữ `.bak` |
| Password/SASL secret | Keychain (`service = KafkaManager`, `account = profile id`) |
| Cert CA/client | Đường dẫn file hoặc Keychain (chốt khi làm mTLS, P2) |
| Preference UI | `UserDefaults` / `@AppStorage` (không chứa secret) |

App Sandbox bật khi đóng gói: cần `com.apple.security.network.client`; truy cập file cert qua `NSOpenPanel` (security-scoped bookmark).

## 10. Chiến lược test

| Lớp | Cách test |
|---|---|
| Model, lọc/sắp xếp, tính lag, decode message | Swift Testing, thuần |
| View model | `FakeKafkaClient` (có độ trễ/lỗi bơm được), kiểm tra state, huỷ Task |
| `GuardedKafkaClient` | Từng method ghi bị chặn khi read-only |
| Stores | Round-trip, file hỏng, Keychain in-memory |
| `LiveKafkaClient` | Integration test với Kafka local (Docker), tách target/flag, không chạy mặc định |
| UI | `#Preview` với Fake; smoke thủ công |

## 11. Quyết định

### Đã chốt

| # | Quyết định | Lý do |
|---|---|---|
| D1 | SwiftUI + `@Observable`, Swift 6 strict concurrency | Native, hiện đại, ít boilerplate |
| D2 | Protocol `KafkaClient` + Fake + Guarded | Làm UI/test trước, đổi client dễ, an toàn ở Core |
| D3 | Secret trong Keychain | Yêu cầu bảo mật cơ bản |
| D4 | Swift Testing | Chuẩn mới của Swift |
| D5 | Xem message dùng consumer không commit offset | Không ảnh hưởng group thật |

### Còn mở

**O1 · Kafka client** — cần một client có **cả admin API** (list/describe/create/delete topic, list/describe group, config) **lẫn consume**.

| Lựa chọn | Ưu | Nhược / cần kiểm chứng |
|---|---|---|
| librdkafka qua C target tự bọc (system lib hoặc XCFramework) | Đủ tính năng: consumer, producer, **AdminClient**; trưởng thành; kiểm soát hoàn toàn | Phải tự bọc C API sang Swift; build/ship thư viện C, ký, universal binary |
| `swift-kafka-client` (swift-server) | API Swift, async/await, xây trên librdkafka | Cần kiểm chứng có admin API hay không; nếu không, vẫn phải tự bọc phần admin |
| Gọi CLI/Java Admin API qua process phụ | Nhanh để thử | Nặng, phụ thuộc JRE, khó đóng gói — không khuyến khích |
| Kafka REST Proxy | Không cần client native | Cần dựng thêm hạ tầng, không phải cluster nào cũng có — không phù hợp |

Khuynh hướng: **librdkafka (bọc tự làm hoặc qua `swift-kafka-client` nếu đủ admin)**. Cần spike ngắn: kết nối broker local, list topic, đọc 10 message, lấy lag một group — để xác nhận trước khi cam kết. Ghi kết quả vào `plans/`.

**O2 · Đóng gói** — SwiftPM executable không có entitlements/signing. Cần Xcode project (hoặc XcodeGen) với App Sandbox, Developer ID, notarization trước khi phát hành. Quyết định khi gần MVP; trong lúc dev dùng `swift run`.

**O3 · Deployment target** — hiện macOS 14; skill `swiftui-macos` nhắm macOS 26. Chốt theo nhóm người dùng.

**O4 · Schema Registry (P2)** — dùng URLSession tự viết hay thư viện; chọn khi làm tính năng.

## 12. Rủi ro

| Rủi ro | Giảm thiểu |
|---|---|
| Client Swift không đủ admin API | Spike O1 sớm; protocol cho phép ghép nhiều nguồn (consume bằng lib A, admin bằng lib B) |
| Thư viện C làm phức tạp build/ký | Cô lập trong `KafkaManagerKafka`; thử đóng gói sớm với một target nhỏ |
| UI đơ khi message lớn / topic nhiều | Giới hạn, batch cập nhật, ảo hoá danh sách (xem §6, §7) |
| Làm lệch offset group thật | D5 + test tích hợp kiểm tra bằng `kafka-consumer-groups.sh` |
| Thao tác nhầm trên production | Read-only mặc định + `GuardedKafkaClient` + gõ lại tên để xác nhận |
