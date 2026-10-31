# 01 · Connection profiles

## Mục tiêu

Lưu nhiều cluster, kết nối an toàn, secret nằm trong Keychain.

## Hành vi / UI

- Sidebar liệt kê cluster; nút `+` mở form thêm/sửa.
- Form: **Name**, **Bootstrap servers** (`host:port,host:port`), **Environment** (Local / Staging / Production → màu + nhãn), **Security** (Plaintext, SSL, SASL_PLAINTEXT, SASL_SSL), **SASL mechanism** (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512), username/password, tuỳ chọn CA cert.
- Nút **Test connection**: hiện spinner, rồi báo "OK, N brokers" hoặc `KafkaError.userMessage`. Timeout cố định (vd. 10s).
- Chọn cluster → kết nối lười (khi mở topic/group đầu tiên), hiện trạng thái (connecting / connected / error + retry) ở sidebar.
- Sửa, nhân bản, xoá profile (xoá có xác nhận; xoá luôn secret trong Keychain).

## Lưu trữ (chốt 2026-10-31: SQLite)

- Profile (không chứa secret): **SQLite** tại `~/Library/Application Support/KafkaManager/kafkamanager.sqlite`, dùng `libsqlite3` có sẵn của macOS (`import SQLite3`), **không thêm dependency**. Lý do: nhẹ (không tốn gì khi chưa mở), một nơi chung cho dữ liệu cục bộ về sau (nhật ký thao tác, cài đặt), ghi atomic có transaction.
- `SQLiteConnectionStore` là `actor` (handle `OpaquePointer` không rời actor), cài đặt protocol `ConnectionStore`; phần còn lại của app chỉ biết protocol (đổi lại JSON/khác không ảnh hưởng UI).
- Schema + migration bằng `PRAGMA user_version`:

```sql
CREATE TABLE connections (
  id TEXT PRIMARY KEY,            -- UUID
  name TEXT NOT NULL,
  bootstrap_servers TEXT NOT NULL,
  environment TEXT NOT NULL DEFAULT 'local',
  security TEXT NOT NULL DEFAULT 'plaintext',
  sasl_mechanism TEXT,
  username TEXT,
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
```

- Mở DB lười ở lần đọc đầu (đọc danh sách profile để vẽ sidebar), `journal_mode=WAL`, `foreign_keys=ON`; câu lệnh dùng prepared statement + bind tham số (không nối chuỗi).
- File hỏng/không mở được: đổi tên thành `kafkamanager.sqlite.corrupt-<timestamp>`, tạo DB mới, báo lỗi cho UI (không crash, không mất file cũ).
- Secret: Keychain (`kSecClassGenericPassword`, service `KafkaManager`, account = profile id). `SecretStore` protocol để test bằng bản in-memory. **Không bao giờ** ghi password vào SQLite.
- `ClusterConnection` mở rộng (đã có từ T01): `environment`, `security`, `saslMechanism`, `username` (không có password).

## Core API

```swift
protocol ConnectionStore { func load() async throws -> [ClusterConnection]; func save(_ c: ClusterConnection) async throws; func delete(id: UUID) async throws }
protocol SecretStore: Sendable { func password(for id: UUID) async throws -> String?; func setPassword(_ password: String, for id: UUID) async throws; func deletePassword(for id: UUID) async throws }
```

## Edge case

- Bootstrap servers sai định dạng → validate ngay trong form, không cho Save.
- Tên trùng → cho phép nhưng cảnh báo.
- DB hỏng → xem mục Lưu trữ (đổi tên file, tạo mới, báo lỗi).
- Keychain bị từ chối → báo rõ, không lưu password.
- Cert không hợp lệ/hết hạn → lỗi `authenticationFailed` kèm gợi ý.

## Tiêu chí hoàn thành

- Thêm, sửa, xoá profile; khởi động lại app vẫn còn.
- Password không xuất hiện trong `connections.json`, log hay UserDefaults.
- Test connection cho 3 kết quả: OK, unreachable, auth failed (với Fake và với broker local thật).

## Test

- `SQLiteConnectionStore` (DB `:memory:` và file tạm): round-trip, sửa, xoá, thứ tự, migration từ `user_version` 0, DB hỏng, truy cập đồng thời.
- Validate bootstrap servers (bảng input hợp lệ/không).
- `SecretStore` in-memory; xoá profile xoá secret.
