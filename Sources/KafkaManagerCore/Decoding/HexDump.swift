import Foundation

enum HexDump {
    static let bytesPerLine = 16
    private static let digits = Array("0123456789abcdef".utf8)

    /// `hexdump -C` layout: "00000000  48 65 6c 6c 6f 20 77 6f  72 6c 64 0a 00 01 02 03  |Hello world.....|", one line
    /// per 16 bytes, a gap after the eighth byte, short last line padded so the ASCII column lines up.
    static func dump(_ bytes: UnsafeBufferPointer<UInt8>, count: Int) -> String {
        let count = min(count, bytes.count)
        guard count > 0 else { return "" }
        let lines = (count + bytesPerLine - 1) / bytesPerLine
        // offset(8) + 2 + 16*3 + 1 + "|" + 16 + "|" + newline
        var out: [UInt8] = []
        out.reserveCapacity(lines * 79)
        var offset = 0
        while offset < count {
            appendHex(UInt32(truncatingIfNeeded: offset), width: 8, to: &out)
            out.append(0x20); out.append(0x20)
            let lineEnd = min(offset + bytesPerLine, count)
            for position in offset..<(offset + bytesPerLine) {
                if position < lineEnd {
                    let byte = bytes[position]
                    out.append(digits[Int(byte >> 4)])
                    out.append(digits[Int(byte & 0x0F)])
                } else {
                    out.append(0x20); out.append(0x20)
                }
                out.append(0x20)
                if position == offset + 7 { out.append(0x20) }
            }
            out.append(0x20)
            out.append(UInt8(ascii: "|"))
            for position in offset..<lineEnd {
                let byte = bytes[position]
                out.append(byte >= 0x20 && byte < 0x7F ? byte : UInt8(ascii: "."))
            }
            out.append(UInt8(ascii: "|"))
            offset = lineEnd
            if offset < count { out.append(0x0A) }
        }
        return String(decoding: out, as: UTF8.self)
    }

    /// "48 65 6c" for at most `count` bytes.
    static func inline(_ bytes: UnsafeBufferPointer<UInt8>, count: Int) -> String {
        let count = min(count, bytes.count)
        var out: [UInt8] = []
        out.reserveCapacity(count * 3)
        for index in 0..<count {
            if index > 0 { out.append(0x20) }
            out.append(digits[Int(bytes[index] >> 4)])
            out.append(digits[Int(bytes[index] & 0x0F)])
        }
        return String(decoding: out, as: UTF8.self)
    }

    private static func appendHex(_ value: UInt32, width: Int, to out: inout [UInt8]) {
        for shift in stride(from: (width - 1) * 4, through: 0, by: -4) {
            out.append(digits[Int((value >> UInt32(shift)) & 0xF)])
        }
    }
}
