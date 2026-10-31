import Foundation

/// Turns message bytes into something to show. Pure and total: any bytes in, a `DecodedPayload` out, never a throw.
///
/// Limits (all in bytes of the payload unless stated):
/// - `limit` bounds what is examined for display: text and hex show at most this many bytes, JSON output stops after
///   this many bytes (the input is still validated end to end).
/// - Synchronous JSON handling is skipped above `syncJSONLimit` (1 MB); `decodeAsync` does it off the caller's actor,
///   up to `asyncJSONLimit`.
/// - No copy of the payload is made: validation and formatting read the `Data` in place, and only the bounded display
///   text is built.
public enum PayloadDecoder {
    public static let displayLimit = 64 * 1024
    public static let previewLimit = 10 * 1024
    public static let syncJSONLimit = 1024 * 1024
    public static let asyncJSONLimit = 32 * 1024 * 1024

    private static let byteOrderMark: [UInt8] = [0xEF, 0xBB, 0xBF]
    private static let previewHexBytes = 16

    // MARK: Decode

    /// - Parameters:
    ///   - data: nil is a null payload (a tombstone value or a missing key).
    ///   - format: `.auto` tries JSON, then UTF-8 text, then hex. An explicit format that does not fit falls back and
    ///     says so in `notes`.
    ///   - limit: see the type's notes. Values below 1 are treated as 1.
    public static func decode(
        _ data: Data?, as format: PayloadFormat = .auto, limit: Int = displayLimit, role: PayloadRole = .value
    ) -> DecodedPayload {
        decode(data, format: format, limit: limit, role: role, jsonByteCap: syncJSONLimit)
    }

    /// Same result as `decode`, but formats JSON of any size up to `asyncJSONLimit` on a background thread, so a big
    /// message never blocks the caller. Cancelling the caller cancels the wait; the background work is short and
    /// bounded by the input size.
    public static func decodeAsync(
        _ data: Data?, as format: PayloadFormat = .auto, limit: Int = displayLimit, role: PayloadRole = .value
    ) async -> DecodedPayload {
        let task = Task.detached(priority: .userInitiated) {
            decode(data, format: format, limit: limit, role: role, jsonByteCap: asyncJSONLimit)
        }
        return await withTaskCancellationHandler { await task.value } onCancel: { task.cancel() }
    }

    private static func decode(_ data: Data?, format: PayloadFormat, limit: Int, role: PayloadRole, jsonByteCap: Int) -> DecodedPayload {
        let limit = max(limit, 1)
        guard let data else {
            let label = role == .value ? String(localized: "null (tombstone)") : String(localized: "null")
            return DecodedPayload(kind: .null, displayText: label, byteCount: 0)
        }
        guard !data.isEmpty else {
            return DecodedPayload(kind: .empty, displayText: String(localized: "(empty)"), byteCount: 0)
        }
        return data.withUnsafeBytes { raw in
            let bytes = raw.bindMemory(to: UInt8.self)
            return Decoding(bytes: bytes, limit: limit, jsonByteCap: jsonByteCap).run(format)
        }
    }

    // MARK: Preview

    /// One line for a table cell: control characters become spaces, long payloads are cut on a character boundary and
    /// end with "…" and the full size. No JSON parsing. Bytes that are not UTF-8 text show as a short hex run.
    public static func preview(_ data: Data?, maxBytes: Int = previewLimit, role: PayloadRole = .value) -> String {
        guard let data else { return role == .value ? String(localized: "null (tombstone)") : String(localized: "null") }
        guard !data.isEmpty else { return String(localized: "(empty)") }
        let maxBytes = max(maxBytes, 1)
        return data.withUnsafeBytes { raw in
            let bytes = raw.bindMemory(to: UInt8.self)
            let start = hasBOM(bytes) ? byteOrderMark.count : 0
            let body = UnsafeBufferPointer(rebasing: bytes[start...])
            let cut = UTF8Validation.boundary(in: body, atMost: maxBytes)
            let shown = UnsafeBufferPointer(rebasing: body[..<cut])
            let truncated = cut < body.count
            let sizeNote = String(localized: "(\(data.count) bytes)")
            if cut > 0, UTF8Validation.isValid(shown), !containsBinaryControl(shown) {
                let line = singleLine(String(decoding: shown, as: UTF8.self))
                return truncated ? "\(line)… \(sizeNote)" : line
            }
            let hexCount = min(previewHexBytes, body.count)
            let hex = HexDump.inline(body, count: hexCount)
            return body.count > hexCount ? "\(hex)… \(sizeNote)" : "\(hex) \(sizeNote)"
        }
    }

    // MARK: Helpers shared with the decoding pass

    fileprivate static func hasBOM(_ bytes: UnsafeBufferPointer<UInt8>) -> Bool {
        bytes.count >= byteOrderMark.count && bytes[0] == byteOrderMark[0] && bytes[1] == byteOrderMark[1] && bytes[2] == byteOrderMark[2]
    }

    /// NUL and the other C0 controls except tab, line feed and carriage return, or DEL: a sign of binary data.
    fileprivate static func containsBinaryControl(_ bytes: UnsafeBufferPointer<UInt8>) -> Bool {
        for byte in bytes where (byte < 0x20 && byte != 0x09 && byte != 0x0A && byte != 0x0D) || byte == 0x7F { return true }
        return false
    }

    private static func singleLine(_ text: String) -> String {
        String(String.UnicodeScalarView(text.unicodeScalars.map { $0.properties.generalCategory == .control ? " " : $0 }))
    }

    // MARK: One decoding pass

    private struct Decoding {
        let bytes: UnsafeBufferPointer<UInt8>
        let limit: Int
        let jsonByteCap: Int

        var total: Int { bytes.count }

        func run(_ format: PayloadFormat) -> DecodedPayload {
            switch format {
            case .hex: return hex(notes: [])
            case .json: return json(explicit: true)
            case .text: return text(explicit: true, notes: [])
            case .auto: return auto()
            }
        }

        // MARK: Auto

        private func auto() -> DecodedPayload {
            var notes: [String] = []
            switch jsonAttempt(&notes) {
            case .success(let payload): return payload
            case .failure: break
            }
            let body = textBody
            if UTF8Validation.isValid(prefix(of: body)) {
                if PayloadDecoder.containsBinaryControl(prefix(of: body)) {
                    return hex(notes: notes + [String(localized: "Contains control characters; shown as hex.")])
                }
                return text(explicit: false, notes: notes)
            }
            return hex(notes: notes + [String(localized: "Invalid UTF-8; shown as hex.")])
        }

        // MARK: JSON

        private enum JSONSkip: Error { case notJSON(reason: String?) }

        /// JSON output, or why it is not (nil reason: simply not JSON).
        private func jsonAttempt(_ notes: inout [String]) -> Result<DecodedPayload, JSONSkip> {
            let body = textBody
            if total > jsonByteCap {
                notes.append(String(localized: "Large payload: JSON formatting skipped."))
                return .failure(.notJSON(reason: notes.last))
            }
            guard UTF8Validation.isValid(body) else { return .failure(.notJSON(reason: nil)) }
            switch JSONPrettyPrinter.format(body, limit: limit) {
            case .success(let output):
                var jsonNotes = bomNotes
                if output.isTruncated { jsonNotes.append(truncationNote(shown: output.text.utf8.count)) }
                return .success(DecodedPayload(
                    kind: .json, displayText: output.text, byteCount: total, isTruncated: output.isTruncated, notes: jsonNotes))
            case .failure(.tooDeep):
                notes.append(String(localized: "JSON nested deeper than \(JSONPrettyPrinter.maxDepth) levels; shown as text."))
                return .failure(.notJSON(reason: notes.last))
            case .failure(.invalid(let offset)):
                return .failure(.notJSON(reason: String(localized: "Not valid JSON (problem near byte \(offset)).")))
            }
        }

        private func json(explicit: Bool) -> DecodedPayload {
            var notes: [String] = []
            switch jsonAttempt(&notes) {
            case .success(let payload):
                return payload
            case .failure(.notJSON(let reason)):
                var fallbackNotes = notes
                if let reason, !fallbackNotes.contains(reason) { fallbackNotes.append(reason) }
                if reason == nil { fallbackNotes.append(String(localized: "Not valid JSON.")) }
                let body = textBody
                if UTF8Validation.isValid(prefix(of: body)), !PayloadDecoder.containsBinaryControl(prefix(of: body)) {
                    return text(explicit: false, notes: fallbackNotes + [String(localized: "Shown as text.")])
                }
                return hex(notes: fallbackNotes + [String(localized: "Shown as hex.")])
            }
        }

        // MARK: Text

        /// Bytes after an optional UTF-8 byte order mark.
        private var textBody: UnsafeBufferPointer<UInt8> {
            PayloadDecoder.hasBOM(bytes) ? UnsafeBufferPointer(rebasing: bytes[3...]) : bytes
        }

        private var bomNotes: [String] {
            PayloadDecoder.hasBOM(bytes) ? [String(localized: "UTF-8 byte order mark removed.")] : []
        }

        /// The first `limit` bytes of `body`, cut on a character boundary.
        private func prefix(of body: UnsafeBufferPointer<UInt8>) -> UnsafeBufferPointer<UInt8> {
            UnsafeBufferPointer(rebasing: body[..<UTF8Validation.boundary(in: body, atMost: limit)])
        }

        /// Explicit Text on invalid UTF-8 shows a lossy decode (invalid bytes as U+FFFD) and control characters as
        /// their "control picture" glyphs (NUL as ␀): the user asked for text, and Hex is one tab away. Auto never
        /// gets here with invalid UTF-8 or controls; it chose hex.
        private func text(explicit: Bool, notes: [String]) -> DecodedPayload {
            let body = textBody
            let shown = prefix(of: body)
            let valid = UTF8Validation.isValid(shown)
            var allNotes = notes + bomNotes
            var display = String(decoding: shown, as: UTF8.self)
            if explicit {
                if !valid { allNotes.append(String(localized: "Invalid UTF-8; bad bytes are shown as �.")) }
                if PayloadDecoder.containsBinaryControl(shown) {
                    display = controlPictures(display)
                    allNotes.append(String(localized: "Control characters are shown as symbols."))
                }
            }
            let truncated = shown.count < body.count
            if truncated { allNotes.append(truncationNote(shown: shown.count)) }
            return DecodedPayload(kind: .text, displayText: display, byteCount: total, isTruncated: truncated, notes: allNotes)
        }

        private func controlPictures(_ text: String) -> String {
            String(String.UnicodeScalarView(text.unicodeScalars.map { scalar in
                switch scalar.value {
                case 0x00...0x08, 0x0B...0x0C, 0x0E...0x1F: Unicode.Scalar(0x2400 + scalar.value) ?? scalar
                case 0x7F: Unicode.Scalar(0x2421) ?? scalar
                default: scalar
                }
            }))
        }

        // MARK: Hex

        private func hex(notes: [String]) -> DecodedPayload {
            let shownCount = min(total, limit)
            let truncated = shownCount < total
            var allNotes = notes
            if truncated { allNotes.append(truncationNote(shown: shownCount)) }
            return DecodedPayload(
                kind: .hex, displayText: HexDump.dump(bytes, count: shownCount), byteCount: total, isTruncated: truncated, notes: allNotes)
        }

        private func truncationNote(shown: Int) -> String {
            let format = ByteCountFormatter()
            format.countStyle = .file
            return String(localized: "Showing the first \(format.string(fromByteCount: Int64(shown))) of \(format.string(fromByteCount: Int64(total))).")
        }
    }
}
