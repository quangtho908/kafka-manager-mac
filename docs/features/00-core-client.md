# 00 · Core client protocol

## Mục tiêu

Tách UI khỏi thư viện Kafka cụ thể. Mọi tính năng chỉ dùng protocol này; client thật (librdkafka…) và client giả (test) cùng conform. Nhờ vậy chưa cần chốt client để bắt đầu viết UI và test.

## Protocol (đề xuất)

```swift
public protocol KafkaClient: Sendable {
    func connect(_ cluster: ClusterConnection, credentials: Credentials?) async throws
    func disconnect() async

    // Topics
    func listTopics() async throws -> [TopicSummary]
    func describeTopic(_ name: String) async throws -> TopicDetail
    func createTopic(_ spec: NewTopic) async throws
    func deleteTopic(_ name: String) async throws

    // Messages
    func fetchMessages(_ query: MessageQuery) -> AsyncThrowingStream<KafkaMessage, Error>

    // Consumer groups
    func listGroups() async throws -> [GroupSummary]
    func describeGroup(_ id: String) async throws -> GroupDetail
}
```

Models (tất cả `Sendable`, `Hashable`): `TopicSummary`, `TopicDetail`, `PartitionInfo`, `NewTopic`, `MessageQuery`, `KafkaMessage`, `GroupSummary`, `GroupDetail`, `GroupMemberInfo`, `PartitionLag`, `Credentials`.

## Lỗi

```swift
public enum KafkaError: Error, Sendable {
    case unreachable(host: String)       // không kết nối được broker
    case authenticationFailed            // sai SASL/cert
    case authorizationFailed(resource: String)
    case timeout
    case unknownTopic(String)
    case topicAlreadyExists(String)
    case clusterError(code: Int, message: String)  // lỗi còn lại, giữ message gốc
}
```

Mỗi case có `userMessage` tiếng Việt/Anh dễ hiểu (đây là chỗ giải quyết "lỗi Kafka khó đọc").

## Triển khai

- `FakeKafkaClient` (trong Core hoặc test support): dữ liệu in-memory, có thể bơm lỗi, độ trễ. Dùng cho test và SwiftUI preview.
- `LiveKafkaClient`: làm sau khi chốt client library (xem `../architecture.md`). Là một `actor` bọc client thật.

## Tiêu chí hoàn thành

- App build và chạy được toàn bộ UI MVP với `FakeKafkaClient`.
- Thay `LiveKafkaClient` không phải sửa view model.

## Test

- `FakeKafkaClient` trả đúng dữ liệu; `KafkaError.userMessage` có giá trị cho mọi case.
