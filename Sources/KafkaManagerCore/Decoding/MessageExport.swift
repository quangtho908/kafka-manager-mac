import Foundation

/// Plain-text forms of a message for the clipboard. Pure; no formatting choices beyond those documented here.
public enum MessageExport {
    public enum Encoding: String, Sendable {
        case utf8, base64
    }

    /// How `rawText` represents the bytes: `utf8` when they are valid UTF-8, otherwise `base64`.
    public static func encoding(of data: Data) -> Encoding {
        String(data: data, encoding: .utf8) == nil ? .base64 : .utf8
    }

    /// The raw payload as text: valid UTF-8 as is (no pretty printing), anything else as base64. nil for a null payload.
    public static func rawText(_ data: Data?) -> String? {
        guard let data else { return nil }
        if let text = String(data: data, encoding: .utf8) { return text }
        return data.base64EncodedString()
    }

    /// The whole message as pretty-printed JSON with sorted keys:
    /// `{"topic", "partition", "offset", "timestamp", "key", "keyEncoding", "value", "valueEncoding", "headers"}`.
    /// - `timestamp` is ISO 8601 in UTC with milliseconds.
    /// - `key` / `value` are null for a null payload (the matching `…Encoding` field is then omitted), the text for
    ///   valid UTF-8, or base64 otherwise; `…Encoding` says which (`"utf8"` or `"base64"`).
    /// - `headers` is a list of `{"key", "value", "encoding"}` in message order, with the same rules for `value`.
    public static func json(_ message: KafkaMessage, topic: String) -> String {
        var object: [String: Any] = [
            "topic": topic,
            "partition": message.partition,
            "offset": message.offset,
            "timestamp": timestamp(message.timestamp),
        ]
        add(message.key, as: "key", to: &object)
        add(message.value, as: "value", to: &object)
        object["headers"] = message.headers.map { header -> [String: Any] in
            var entry: [String: Any] = ["key": header.key]
            add(header.value, as: "value", encodingKey: "encoding", to: &entry)
            return entry
        }
        let options: JSONSerialization.WritingOptions = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        guard let data = try? JSONSerialization.data(withJSONObject: object, options: options),
              let text = String(data: data, encoding: .utf8) else { return "{}" }
        return text
    }

    /// "2023-11-14T22:13:20.123Z" (UTC) or "2023-11-15T05:13:20.123+07:00" in another zone. Milliseconds are rounded
    /// from the date, so a `Date` built from fractional seconds does not lose a millisecond to floating point.
    public static func timestamp(_ date: Date, timeZone: TimeZone = .gmt) -> String {
        let millis = Int64((date.timeIntervalSince1970 * 1000).rounded())
        let seconds = Double(millis.floorDiv(1000))
        let style = Date.ISO8601FormatStyle(timeZone: timeZone)
            .dateSeparator(.dash).timeSeparator(.colon).timeZoneSeparator(.colon)
        let whole = Date(timeIntervalSince1970: seconds).formatted(style)
        let zoneLength = whole.hasSuffix("Z") ? 1 : 6
        let fraction = String(format: ".%03d", Int(millis - millis.floorDiv(1000) * 1000))
        return String(whole.dropLast(zoneLength)) + fraction + String(whole.suffix(zoneLength))
    }

    private static func add(_ data: Data?, as name: String, encodingKey: String? = nil, to object: inout [String: Any]) {
        guard let data else {
            object[name] = NSNull()
            return
        }
        object[name] = rawText(data)
        object[encodingKey ?? name + "Encoding"] = encoding(of: data).rawValue
    }
}

private extension Int64 {
    /// Division that rounds toward negative infinity, so timestamps before 1970 keep a positive millisecond part.
    func floorDiv(_ divisor: Int64) -> Int64 {
        let quotient = self / divisor
        return (self % divisor != 0 && (self < 0) != (divisor < 0)) ? quotient - 1 : quotient
    }
}
