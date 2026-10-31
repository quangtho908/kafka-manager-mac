import Foundation

/// How the text a user types becomes bytes.
public enum ProduceEncoding: String, CaseIterable, Hashable, Sendable {
    /// UTF-8 exactly as typed.
    case text
    /// UTF-8 as typed, after checking it is valid JSON.
    case json
    /// Pairs of hex digits.
    case hex
}

public enum ProduceEncodingIssue: Error, Hashable, Sendable {
    /// `offset` counts characters from the start of the text.
    case invalidHexDigit(offset: Int)
    case oddHexLength
    case invalidJSON(offset: Int)
    case jsonTooDeep

    public var message: String {
        switch self {
        case .invalidHexDigit: String(localized: "Hex can only hold the digits 0–9 and A–F.")
        case .oddHexLength: String(localized: "Hex needs an even number of digits.")
        case .invalidJSON(let offset): String(localized: "Not valid JSON (problem near byte \(offset)).")
        case .jsonTooDeep: String(localized: "The JSON is nested too deeply.")
        }
    }
}

public enum ProduceEncoder {
    /// Bytes for `text`. Hex ignores whitespace and the separators `: - ,`, and accepts a leading `0x`.
    public static func data(from text: String, as encoding: ProduceEncoding) -> Result<Data, ProduceEncodingIssue> {
        switch encoding {
        case .text:
            return .success(Data(text.utf8))
        case .json:
            let bytes = Array(text.utf8)
            let outcome = bytes.withUnsafeBufferPointer { JSONPrettyPrinter.format($0, limit: 0) }
            switch outcome {
            case .success: return .success(Data(bytes))
            case .failure(let failure): return .failure(Self.issue(failure))
            }
        case .hex:
            return hexData(text)
        }
    }

    /// The JSON re-indented (two spaces), or why it can't be.
    public static func formatJSON(_ text: String) -> Result<String, ProduceEncodingIssue> {
        let bytes = Array(text.utf8)
        let outcome = bytes.withUnsafeBufferPointer { JSONPrettyPrinter.format($0, limit: bytes.count * 4 + 1024) }
        switch outcome {
        case .success(let output): return .success(output.text)
        case .failure(let failure): return .failure(Self.issue(failure))
        }
    }

    /// Text for editing existing bytes: the UTF-8 text when it is valid and shows what it holds, otherwise hex. Control
    /// characters other than tab and line breaks would be invisible in a text field, so they send the bytes to hex.
    public static func editableText(for data: Data) -> (text: String, encoding: ProduceEncoding) {
        if let text = String(data: data, encoding: .utf8), !data.contains(where: isHiddenControl) { return (text, .text) }
        return (hexString(data), .hex)
    }

    private static func isHiddenControl(_ byte: UInt8) -> Bool {
        (byte < 0x20 && byte != 0x09 && byte != 0x0A && byte != 0x0D) || byte == 0x7F
    }

    /// "48 65 6c 6c 6f".
    public static func hexString(_ data: Data) -> String {
        let digits = Array("0123456789abcdef")
        var parts: [String] = []
        parts.reserveCapacity(data.count)
        for byte in data { parts.append(String([digits[Int(byte >> 4)], digits[Int(byte & 0x0F)]])) }
        return parts.joined(separator: " ")
    }

    private static func hexData(_ text: String) -> Result<Data, ProduceEncodingIssue> {
        var characters = Array(text.unicodeScalars)
        var skipped = 0
        // Leading whitespace then an optional 0x.
        var start = 0
        while start < characters.count, characters[start].properties.isWhitespace { start += 1 }
        if start + 1 < characters.count, characters[start] == "0", characters[start + 1] == "x" || characters[start + 1] == "X" {
            start += 2
        }
        skipped = start
        characters = Array(characters[start...])
        var nibbles: [UInt8] = []
        nibbles.reserveCapacity(characters.count)
        for (index, scalar) in characters.enumerated() {
            if scalar.properties.isWhitespace || scalar == ":" || scalar == "-" || scalar == "," { continue }
            guard let value = nibble(scalar) else { return .failure(.invalidHexDigit(offset: skipped + index)) }
            nibbles.append(value)
        }
        guard nibbles.count % 2 == 0 else { return .failure(.oddHexLength) }
        var data = Data(capacity: nibbles.count / 2)
        var index = 0
        while index < nibbles.count {
            data.append(nibbles[index] << 4 | nibbles[index + 1])
            index += 2
        }
        return .success(data)
    }

    private static func nibble(_ scalar: Unicode.Scalar) -> UInt8? {
        switch scalar.value {
        case 0x30...0x39: UInt8(scalar.value - 0x30)
        case 0x41...0x46: UInt8(scalar.value - 0x41 + 10)
        case 0x61...0x66: UInt8(scalar.value - 0x61 + 10)
        default: nil
        }
    }

    private static func issue(_ failure: JSONPrettyPrinter.Failure) -> ProduceEncodingIssue {
        switch failure {
        case .invalid(let offset): .invalidJSON(offset: offset)
        case .tooDeep: .jsonTooDeep
        }
    }
}
