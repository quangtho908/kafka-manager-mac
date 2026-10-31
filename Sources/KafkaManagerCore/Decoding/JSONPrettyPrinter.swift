import Foundation

/// Validates JSON (RFC 8259, scalars allowed) and re-indents it in one pass over the bytes. Key order, number
/// literals ("1e999", 12345678901234567890) and string escapes are copied exactly as they are in the source, which
/// JSONSerialization cannot do. No recursion: nesting is tracked on an explicit stack, so depth is bounded by
/// `maxDepth`, not by the thread's stack.
struct JSONPrettyPrinter {
    enum Failure: Error, Equatable {
        case invalid(offset: Int)
        case tooDeep
    }

    struct Output {
        /// Formatted text, cut at `limit` bytes on a character boundary.
        let text: String
        let isTruncated: Bool
    }

    static let maxDepth = 512
    private static let indentWidth = 2

    /// Whether `bytes` is valid JSON, and its pretty-printed form up to `limit` output bytes. The whole input is
    /// always validated; only the output is bounded.
    static func format(_ bytes: UnsafeBufferPointer<UInt8>, limit: Int) -> Result<Output, Failure> {
        var printer = JSONPrettyPrinter(bytes: bytes, limit: limit)
        do {
            try printer.run()
        } catch let failure as Failure {
            return .failure(failure)
        } catch {
            return .failure(.invalid(offset: printer.index))
        }
        let cut = printer.out.withUnsafeBufferPointer { UTF8Validation.lengthWithoutIncompleteTail($0, count: $0.count) }
        return .success(Output(text: String(decoding: printer.out[..<cut], as: UTF8.self), isTruncated: printer.truncated))
    }

    private let bytes: UnsafeBufferPointer<UInt8>
    private let limit: Int
    private var index = 0
    private var out: [UInt8] = []
    private var truncated = false
    private var stack: [Bool] = []   // true = object, false = array

    private init(bytes: UnsafeBufferPointer<UInt8>, limit: Int) {
        self.bytes = bytes
        self.limit = limit
        out.reserveCapacity(min(bytes.count + bytes.count / 4, limit))
    }

    // MARK: Output

    private mutating func emit(_ byte: UInt8) {
        if out.count < limit { out.append(byte) } else { truncated = true }
    }

    private mutating func emit(_ text: StaticString) {
        text.withUTF8Buffer { for byte in $0 { emit(byte) } }
    }

    private mutating func newline() {
        emit(0x0A)
        for _ in 0..<(stack.count * Self.indentWidth) { emit(0x20) }
    }

    // MARK: Scanning

    private var peek: UInt8? { index < bytes.count ? bytes[index] : nil }

    private mutating func skipWhitespace() {
        while index < bytes.count, bytes[index] == 0x20 || bytes[index] == 0x09 || bytes[index] == 0x0A || bytes[index] == 0x0D {
            index += 1
        }
    }

    private func fail() -> Failure { .invalid(offset: index) }

    private mutating func run() throws {
        var expectingValue = true
        while true {
            skipWhitespace()
            if expectingValue {
                guard let byte = peek else { throw fail() }
                switch byte {
                case UInt8(ascii: "{"):
                    index += 1
                    skipWhitespace()
                    if peek == UInt8(ascii: "}") {
                        index += 1
                        emit("{}")
                        expectingValue = false
                    } else {
                        guard stack.count < Self.maxDepth else { throw Failure.tooDeep }
                        stack.append(true)
                        emit("{")
                        newline()
                        try key()
                    }
                case UInt8(ascii: "["):
                    index += 1
                    skipWhitespace()
                    if peek == UInt8(ascii: "]") {
                        index += 1
                        emit("[]")
                        expectingValue = false
                    } else {
                        guard stack.count < Self.maxDepth else { throw Failure.tooDeep }
                        stack.append(false)
                        emit("[")
                        newline()
                    }
                case UInt8(ascii: "\""):
                    try string()
                    expectingValue = false
                case UInt8(ascii: "-"), UInt8(ascii: "0")...UInt8(ascii: "9"):
                    try number()
                    expectingValue = false
                case UInt8(ascii: "t"):
                    try literal("true")
                    expectingValue = false
                case UInt8(ascii: "f"):
                    try literal("false")
                    expectingValue = false
                case UInt8(ascii: "n"):
                    try literal("null")
                    expectingValue = false
                default:
                    throw fail()
                }
            } else {
                guard let isObject = stack.last else {
                    // Top-level value done: only whitespace may follow.
                    if index == bytes.count { return }
                    throw fail()
                }
                guard let byte = peek else { throw fail() }
                if byte == UInt8(ascii: ",") {
                    index += 1
                    emit(",")
                    newline()
                    if isObject {
                        skipWhitespace()
                        try key()
                    }
                    expectingValue = true
                } else if byte == (isObject ? UInt8(ascii: "}") : UInt8(ascii: "]")) {
                    index += 1
                    stack.removeLast()
                    newline()
                    emit(byte)
                } else {
                    throw fail()
                }
            }
        }
    }

    /// `"name": ` — leaves the scanner ready for the member's value.
    private mutating func key() throws {
        guard peek == UInt8(ascii: "\"") else { throw fail() }
        try string()
        skipWhitespace()
        guard peek == UInt8(ascii: ":") else { throw fail() }
        index += 1
        emit(": ")
    }

    private mutating func literal(_ word: StaticString) throws {
        let length = word.utf8CodeUnitCount
        guard index + length <= bytes.count else { throw fail() }
        var matches = true
        word.withUTF8Buffer { expected in
            for offset in 0..<length where bytes[index + offset] != expected[offset] { matches = false }
        }
        guard matches else { throw fail() }
        // "nullx" is not a literal followed by garbage that the caller would reject anyway, but fail here for a
        // precise offset.
        for _ in 0..<length { emit(bytes[index]); index += 1 }
    }

    private mutating func string() throws {
        emit(bytes[index])
        index += 1
        while true {
            guard let byte = peek else { throw fail() }
            if byte < 0x20 { throw fail() }
            emit(byte)
            index += 1
            if byte == UInt8(ascii: "\"") { return }
            if byte == UInt8(ascii: "\\") {
                guard let escape = peek else { throw fail() }
                switch escape {
                case UInt8(ascii: "\""), UInt8(ascii: "\\"), UInt8(ascii: "/"), UInt8(ascii: "b"),
                     UInt8(ascii: "f"), UInt8(ascii: "n"), UInt8(ascii: "r"), UInt8(ascii: "t"):
                    emit(escape)
                    index += 1
                case UInt8(ascii: "u"):
                    emit(escape)
                    index += 1
                    for _ in 0..<4 {
                        guard let digit = peek, Self.isHexDigit(digit) else { throw fail() }
                        emit(digit)
                        index += 1
                    }
                default:
                    throw fail()
                }
            }
        }
    }

    /// -? (0 | [1-9][0-9]*) (. [0-9]+)? ([eE] [+-]? [0-9]+)?  — copied verbatim.
    private mutating func number() throws {
        if peek == UInt8(ascii: "-") { emit(0x2D); index += 1 }
        guard let first = peek, Self.isDigit(first) else { throw fail() }
        if first == UInt8(ascii: "0") {
            emit(first); index += 1
        } else {
            try digits()
        }
        if peek == UInt8(ascii: ".") {
            emit(0x2E); index += 1
            try digits()
        }
        if let e = peek, e == UInt8(ascii: "e") || e == UInt8(ascii: "E") {
            emit(e); index += 1
            if let sign = peek, sign == UInt8(ascii: "+") || sign == UInt8(ascii: "-") { emit(sign); index += 1 }
            try digits()
        }
    }

    private mutating func digits() throws {
        guard let first = peek, Self.isDigit(first) else { throw fail() }
        while let byte = peek, Self.isDigit(byte) { emit(byte); index += 1 }
    }

    private static func isDigit(_ byte: UInt8) -> Bool { byte >= 0x30 && byte <= 0x39 }

    private static func isHexDigit(_ byte: UInt8) -> Bool {
        isDigit(byte) || (byte >= 0x41 && byte <= 0x46) || (byte >= 0x61 && byte <= 0x66)
    }
}
