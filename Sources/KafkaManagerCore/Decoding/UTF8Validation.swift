import Foundation

enum UTF8Validation {
    /// Strict validation: no overlong forms, no surrogates (U+D800…DFFF), nothing above U+10FFFF.
    static func isValid(_ bytes: UnsafeBufferPointer<UInt8>) -> Bool {
        var i = 0
        let n = bytes.count
        while i < n {
            let b0 = bytes[i]
            if b0 < 0x80 { i += 1; continue }
            let length: Int
            switch b0 {
            case 0xC2...0xDF: length = 2
            case 0xE0...0xEF: length = 3
            case 0xF0...0xF4: length = 4
            default: return false
            }
            guard i + length <= n else { return false }
            let b1 = bytes[i + 1]
            switch b0 {
            case 0xE0: guard (0xA0...0xBF).contains(b1) else { return false }
            case 0xED: guard (0x80...0x9F).contains(b1) else { return false }
            case 0xF0: guard (0x90...0xBF).contains(b1) else { return false }
            case 0xF4: guard (0x80...0x8F).contains(b1) else { return false }
            default: guard isContinuation(b1) else { return false }
            }
            for offset in 2..<max(length, 2) where !isContinuation(bytes[i + offset]) { return false }
            i += length
        }
        return true
    }

    static func isContinuation(_ byte: UInt8) -> Bool { byte & 0xC0 == 0x80 }

    /// The largest length ≤ `maxLength` at which `bytes` can be cut without splitting a UTF-8 character.
    static func boundary(in bytes: UnsafeBufferPointer<UInt8>, atMost maxLength: Int) -> Int {
        var length = min(maxLength, bytes.count)
        guard length < bytes.count else { return length }
        // At most three continuation bytes can belong to the character that straddles the cut.
        var steps = 0
        while length > 0, steps < 3, isContinuation(bytes[length]) {
            length -= 1
            steps += 1
        }
        return length
    }

    /// `count` minus a character cut off at the end, so a prefix of valid UTF-8 stays valid.
    static func lengthWithoutIncompleteTail(_ bytes: UnsafeBufferPointer<UInt8>, count: Int) -> Int {
        let count = min(count, bytes.count)
        var start = count - 1
        var steps = 0
        while start > 0, steps < 3, isContinuation(bytes[start]) {
            start -= 1
            steps += 1
        }
        guard start >= 0, count > 0 else { return count }
        let lead = bytes[start]
        let needed: Int
        switch lead {
        case 0xC2...0xDF: needed = 2
        case 0xE0...0xEF: needed = 3
        case 0xF0...0xF4: needed = 4
        default: return count
        }
        return start + needed > count ? start : count
    }
}
