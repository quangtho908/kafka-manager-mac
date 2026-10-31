import Testing
import Foundation
@testable import KafkaManagerCore

private func data(_ text: String) -> Data { Data(text.utf8) }
private func decode(_ text: String, as format: PayloadFormat = .auto, limit: Int = PayloadDecoder.displayLimit) -> DecodedPayload {
    PayloadDecoder.decode(data(text), as: format, limit: limit)
}
private let bom = Data([0xEF, 0xBB, 0xBF])

@Suite("PayloadDecoder basics")
struct PayloadDecoderBasicsTests {
    @Test func nilValueIsATombstoneAndNilKeyIsNull() {
        let value = PayloadDecoder.decode(nil)
        #expect(value.kind == .null && value.displayText == "null (tombstone)" && value.byteCount == 0)
        let key = PayloadDecoder.decode(nil, role: .key)
        #expect(key.kind == .null && key.displayText == "null")
        for format in PayloadFormat.allCases {
            #expect(PayloadDecoder.decode(nil, as: format).kind == .null)
        }
    }

    @Test func emptyDataIsLabelledForEveryFormat() {
        for format in PayloadFormat.allCases {
            let payload = PayloadDecoder.decode(Data(), as: format)
            #expect(payload.kind == .empty && payload.displayText == "(empty)" && payload.byteCount == 0 && !payload.isTruncated)
        }
    }

    @Test func oneByteInputs() {
        #expect(PayloadDecoder.decode(Data([0x61])).kind == .text)
        #expect(PayloadDecoder.decode(Data([0x80])).kind == .hex)
        #expect(PayloadDecoder.decode(Data([0x00])).kind == .hex)
        #expect(PayloadDecoder.decode(Data([0x37])).kind == .json)   // the number 7... as a digit "7"
    }

    @Test func limitBelowOneIsTreatedAsOne() {
        let payload = PayloadDecoder.decode(data("abc"), as: .text, limit: 0)
        #expect(payload.displayText == "a" && payload.isTruncated)
    }
}

@Suite("PayloadDecoder JSON")
struct PayloadDecoderJSONTests {
    @Test func objectKeepsKeyOrderAndIsIndentedByTwo() {
        let payload = decode(#"{"b":1,"a":[1,2,{"c":null}],"e":{},"f":[],"d":"x y"}"#)
        #expect(payload.kind == .json)
        #expect(payload.displayText == """
        {
          "b": 1,
          "a": [
            1,
            2,
            {
              "c": null
            }
          ],
          "e": {},
          "f": [],
          "d": "x y"
        }
        """)
        #expect(payload.notes.isEmpty && !payload.isTruncated)
    }

    @Test func alreadyIndentedJSONIsNormalised() {
        #expect(decode("  {\n \"a\" :  1 ,\t\"b\":[ ]\r\n}  ").displayText == "{\n  \"a\": 1,\n  \"b\": []\n}")
    }

    @Test(arguments: [("42", "42"), ("-0.5e+10", "-0.5e+10"), (#""hi""#, #""hi""#), ("true", "true"), ("false", "false"), ("null", "null"), ("[]", "[]"), ("{}", "{}")])
    func scalarsAndEmptyContainersAreJSON(input: String, expected: String) {
        let payload = decode(input)
        #expect(payload.kind == .json && payload.displayText == expected)
    }

    @Test func bigNumbersKeepEveryDigit() {
        let payload = decode("[1e999, 12345678901234567890123456789, -0.000000000000000000001, 1E-400]")
        #expect(payload.kind == .json)
        #expect(payload.displayText.contains("1e999"))
        #expect(payload.displayText.contains("12345678901234567890123456789"))
        #expect(payload.displayText.contains("-0.000000000000000000001"))
        #expect(payload.displayText.contains("1E-400"))
    }

    @Test func unicodeEscapesAndRawUnicodeSurvive() {
        let payload = decode(#"{"k":"café 😀 \n \" \\ \/","emoji":"😀é"}"#)
        #expect(payload.kind == .json)
        #expect(payload.displayText.contains(#"café 😀 \n \" \\ \/"#))
        #expect(payload.displayText.contains("😀é"))
    }

    @Test(arguments: [
        #"{"a":}"#, #"{"a" 1}"#, "{a:1}", "[1,]", "[1 2]", "{,}", "'x'", "01", "1.", ".5", "-", "+1", "NaN", "Infinity",
        "tru", "nulll", "[1] x", "{} {}", #"{"a":1,}"#, #""unterminated"#, #""bad \x escape""#, #""\u12G4""#,
        "\"tab\there\"", "[", "]", "{", "", " ",
    ])
    func invalidJSONFallsBackToTextWithoutThrowing(input: String) {
        guard !input.isEmpty else { return }
        let auto = decode(input)
        #expect(auto.kind != .json)
        let explicit = decode(input, as: .json)
        #expect(explicit.kind == .text)
        #expect(explicit.displayText == input)
        #expect(!explicit.notes.isEmpty)
    }

    @Test func explicitJSONOnBinaryFallsBackToHexWithANote() {
        let payload = PayloadDecoder.decode(Data([0xFF, 0x00, 0x7B]), as: .json)
        #expect(payload.kind == .hex && !payload.notes.isEmpty)
    }

    @Test func nestingUpToTheCapIsFormattedAndBeyondFallsBackToText() {
        let ok = String(repeating: "[", count: JSONPrettyPrinter.maxDepth) + String(repeating: "]", count: JSONPrettyPrinter.maxDepth)
        #expect(decode(ok).kind == .json)
        let deep = String(repeating: "[", count: 1000) + String(repeating: "]", count: 1000)
        let payload = decode(deep)
        #expect(payload.kind == .text && payload.displayText == deep && !payload.notes.isEmpty)
        let deepObjects = String(repeating: #"{"a":"#, count: 1000) + "1" + String(repeating: "}", count: 1000)
        #expect(decode(deepObjects).kind == .text)
        #expect(decode(deepObjects, as: .json).kind == .text)
    }

    @Test func unbalancedDeepNestingDoesNotCrash() {
        #expect(decode(String(repeating: "[", count: 100_000)).kind == .text)
        #expect(decode(String(repeating: "{\"a\":", count: 100_000)).kind == .text)
    }

    @Test func byteOrderMarkIsStrippedWithANote() {
        let payload = PayloadDecoder.decode(bom + data(#"{"a":1}"#))
        #expect(payload.kind == .json && payload.displayText == "{\n  \"a\": 1\n}")
        #expect(payload.byteCount == 10 && !payload.notes.isEmpty)
    }

    @Test func jsonOutputIsCutAtTheLimitOnACharacterBoundary() {
        let payload = decode(#"{"k":"ééééééééééééé","z":[1,2,3]}"#, limit: 15)
        #expect(payload.kind == .json && payload.isTruncated)
        #expect(payload.displayText.utf8.count <= 15)
        #expect(!payload.displayText.contains("\u{FFFD}"))
        #expect(!payload.notes.isEmpty)
    }

    @Test func invalidJSONBeyondTheLimitIsStillDetected() {
        let input = "[" + String(repeating: "1,", count: 100) + "]"   // trailing comma at the end
        #expect(decode(input, limit: 20).kind == .text)
    }
}

@Suite("PayloadDecoder text and binary")
struct PayloadDecoderTextTests {
    @Test(arguments: ["hello", "line one\nline two\r\n\ttabbed", "café", "日本語のテキスト", "emoji 😀👨‍👩‍👧 flag 🇻🇳", "just words and: {braces", "[not json"])
    func utf8TextIsText(input: String) {
        let payload = decode(input)
        #expect(payload.kind == .text && payload.displayText == input && payload.byteCount == input.utf8.count)
        #expect(!payload.isTruncated && payload.notes.isEmpty)
    }

    @Test(arguments: [
        [0xFF, 0xFE, 0x41], [0xC0, 0xAF], [0xED, 0xA0, 0x80], [0xE2, 0x82], [0xF4, 0x90, 0x80, 0x80], [0x80], [0xC3],
        [0x68, 0x69, 0xFF],
    ] as [[UInt8]])
    func invalidUTF8FallsBackToHexInAuto(bytes: [UInt8]) {
        let payload = PayloadDecoder.decode(Data(bytes))
        #expect(payload.kind == .hex)
        #expect(!payload.notes.isEmpty)
        #expect(payload.byteCount == bytes.count)
    }

    @Test func explicitTextOnInvalidUTF8IsLossyWithANote() {
        let payload = PayloadDecoder.decode(Data([0x68, 0x69, 0xFF, 0x21]), as: .text)
        #expect(payload.kind == .text && payload.displayText == "hi\u{FFFD}!" && !payload.notes.isEmpty)
    }

    @Test func nulAndOtherControlBytesMeanBinaryInAutoButShowAsSymbolsInText() {
        let bytes = Data([0x61, 0x00, 0x62])
        #expect(PayloadDecoder.decode(bytes).kind == .hex)
        let text = PayloadDecoder.decode(bytes, as: .text)
        #expect(text.kind == .text && text.displayText == "a\u{2400}b")
        #expect(PayloadDecoder.decode(Data([0x01, 0x02, 0x03])).kind == .hex)
        #expect(PayloadDecoder.decode(data("tab\there\nnewline")).kind == .text)
    }

    @Test func byteOrderMarkIsStrippedFromText() {
        let payload = PayloadDecoder.decode(bom + data("hello"))
        #expect(payload.kind == .text && payload.displayText == "hello" && payload.byteCount == 8 && !payload.notes.isEmpty)
        let hex = PayloadDecoder.decode(bom + data("hello"), as: .hex)
        #expect(hex.displayText.contains("ef bb bf"))
    }

    @Test func textIsCutOnACharacterBoundary() {
        let two = decode("ééé", as: .text, limit: 3)
        #expect(two.displayText == "é" && two.isTruncated && two.byteCount == 6)
        let four = decode("😀😀", as: .text, limit: 5)
        #expect(four.displayText == "😀" && four.isTruncated)
        let tiny = decode("😀", as: .text, limit: 2)
        #expect(tiny.displayText.isEmpty && tiny.isTruncated)
        for limit in 1...9 {
            let payload = decode("a😀é日", as: .text, limit: limit)
            #expect(!payload.displayText.contains("\u{FFFD}"), "limit \(limit)")
        }
    }

    @Test func randomBytesNeverCrashInAnyFormat() {
        var generator = SystemRandomNumberGenerator()
        for _ in 0..<500 {
            let length = Int.random(in: 0...300, using: &generator)
            let bytes = Data((0..<length).map { _ in UInt8.random(in: 0...255, using: &generator) })
            for format in PayloadFormat.allCases {
                let payload = PayloadDecoder.decode(bytes, as: format, limit: Int.random(in: 1...400, using: &generator))
                #expect(payload.byteCount == length)
            }
            _ = PayloadDecoder.preview(bytes, maxBytes: Int.random(in: 1...400, using: &generator))
        }
    }
}

@Suite("PayloadDecoder hex")
struct PayloadDecoderHexTests {
    @Test func canonicalLayoutMatchesHexdumpC() {
        let bytes = Data("Hello world\n".utf8) + Data([0, 1, 2, 3]) + Data("A".utf8)
        let payload = PayloadDecoder.decode(bytes, as: .hex)
        let expected = "00000000  48 65 6c 6c 6f 20 77 6f  72 6c 64 0a 00 01 02 03  |Hello world.....|\n"
            + "00000010  41" + String(repeating: " ", count: 48) + "|A|"
        #expect(payload.kind == .hex && payload.displayText == expected && payload.byteCount == 17 && !payload.isTruncated)
    }

    @Test func fullLineAndOffsetsGrow() {
        let bytes = Data((0..<48).map { UInt8($0) })
        let lines = PayloadDecoder.decode(bytes, as: .hex).displayText.split(separator: "\n")
        #expect(lines.count == 3)
        #expect(lines[0].hasPrefix("00000000  00 01 02 03 04 05 06 07  08 09 0a 0b 0c 0d 0e 0f  |"))
        #expect(lines[2].hasPrefix("00000020  "))
        #expect(lines.allSatisfy { $0.count == 78 })
    }

    @Test func nonPrintableBytesAreDotsInTheAsciiColumn() {
        let payload = PayloadDecoder.decode(Data([0x7F, 0x80, 0xFF, 0x1F, 0x20, 0x7E]), as: .hex)
        #expect(payload.displayText.hasSuffix("|.... ~|"))
    }

    @Test func hexStopsAtTheLimit() {
        let bytes = Data(repeating: 0xAB, count: 100)
        let payload = PayloadDecoder.decode(bytes, as: .hex, limit: 32)
        #expect(payload.displayText.split(separator: "\n").count == 2)
        #expect(payload.isTruncated && payload.byteCount == 100 && !payload.notes.isEmpty)
        let partial = PayloadDecoder.decode(bytes, as: .hex, limit: 20)
        #expect(partial.displayText.split(separator: "\n").count == 2)
    }
}

@Suite("PayloadDecoder large payloads")
struct PayloadDecoderLargeTests {
    private static let fiveMB = 5 * 1024 * 1024

    private static func bigJSON(bytes: Int) -> Data {
        var data = Data("[".utf8)
        let item = Data("0,".utf8)
        data.reserveCapacity(bytes)
        while data.count < bytes - 8 { data.append(item) }
        data.append(Data("0]".utf8))
        return data
    }

    @Test func fiveMegabytesOfBinaryIsTruncatedQuickly() {
        let bytes = Data((0..<Self.fiveMB).map { UInt8(truncatingIfNeeded: $0 &* 31) })
        let clock = ContinuousClock()
        var payload: DecodedPayload?
        let elapsed = clock.measure { payload = PayloadDecoder.decode(bytes, as: .auto) }
        #expect(payload?.kind == .hex)
        #expect(payload?.isTruncated == true && payload?.byteCount == Self.fiveMB)
        #expect((payload?.displayText.utf8.count ?? 0) < 6 * PayloadDecoder.displayLimit)
        #expect(elapsed < .seconds(5), "took \(elapsed)")
    }

    @Test func fiveMegabytesOfTextOnlyShowsAPrefix() {
        let bytes = Data(repeating: UInt8(ascii: "x"), count: Self.fiveMB)
        let payload = PayloadDecoder.decode(bytes, as: .text)
        #expect(payload.kind == .text && payload.isTruncated)
        #expect(payload.displayText.utf8.count == PayloadDecoder.displayLimit)
        #expect(payload.byteCount == Self.fiveMB)
    }

    @Test func syncAutoSkipsJSONAboveOneMegabyte() {
        let big = Self.bigJSON(bytes: Self.fiveMB)
        let payload = PayloadDecoder.decode(big)
        #expect(payload.kind == .text && payload.isTruncated)
        #expect(!payload.notes.isEmpty)
        #expect(PayloadDecoder.decode(big, as: .json).kind == .text)
        #expect(PayloadDecoder.decode(Self.bigJSON(bytes: PayloadDecoder.syncJSONLimit)).kind == .json)
    }

    @Test func asyncFormatsBigJSONAndBoundsTheOutput() async {
        let big = Self.bigJSON(bytes: Self.fiveMB)
        let clock = ContinuousClock()
        var payload: DecodedPayload?
        let elapsed = await clock.measure { payload = await PayloadDecoder.decodeAsync(big) }
        #expect(payload?.kind == .json && payload?.isTruncated == true)
        #expect((payload?.displayText.utf8.count ?? .max) <= PayloadDecoder.displayLimit)
        #expect(payload?.byteCount == big.count)
        #expect(elapsed < .seconds(30), "took \(elapsed)")
    }

    @Test func asyncAgreesWithSyncOnSmallInputs() async {
        for text in [#"{"a":[1,2]}"#, "plain", "\u{0}\u{1}"] {
            #expect(await PayloadDecoder.decodeAsync(data(text)) == PayloadDecoder.decode(data(text)))
        }
        #expect(await PayloadDecoder.decodeAsync(nil, role: .key).displayText == "null")
    }

    @MainActor @Test func asyncDecodeDoesNotBlockTheMainActor() async {
        let big = Self.bigJSON(bytes: 8 * 1024 * 1024)
        let work = Task { await PayloadDecoder.decodeAsync(big) }
        var ticks = 0
        var finished = false
        let watcher = Task { _ = await work.value; finished = true }
        while !finished {
            ticks += 1
            try? await Task.sleep(for: .milliseconds(5))
        }
        _ = await watcher.value
        // A blocked main actor would have managed one iteration; a free one keeps ticking while the work runs.
        #expect(ticks > 3, "main actor ticked only \(ticks) times")
        #expect(await work.value.kind == .json)
    }
}

@Suite("PayloadDecoder preview")
struct PayloadDecoderPreviewTests {
    @Test func nullEmptyAndShortText() {
        #expect(PayloadDecoder.preview(nil) == "null (tombstone)")
        #expect(PayloadDecoder.preview(nil, role: .key) == "null")
        #expect(PayloadDecoder.preview(Data()) == "(empty)")
        #expect(PayloadDecoder.preview(data("hello")) == "hello")
    }

    @Test func newlinesAndTabsBecomeSpacesSoItStaysOneLine() {
        let line = PayloadDecoder.preview(data("a\nb\tc\r\nd"))
        #expect(line == "a b c  d")
        #expect(!line.contains("\n"))
    }

    @Test func longTextEndsWithEllipsisAndTheFullSize() {
        let line = PayloadDecoder.preview(data(String(repeating: "x", count: 100)), maxBytes: 10)
        #expect(line.hasPrefix("xxxxxxxxxx…"))
        #expect(line.contains("100"))
    }

    @Test func neverSplitsAMultiByteCharacter() {
        for limit in 1...12 {
            let line = PayloadDecoder.preview(data("a😀é日b😀"), maxBytes: limit)
            #expect(!line.contains("\u{FFFD}"), "limit \(limit)")
        }
        #expect(PayloadDecoder.preview(data("😀😀"), maxBytes: 5).hasPrefix("😀…"))
    }

    @Test func doesNotParseJSON() {
        #expect(PayloadDecoder.preview(data(#"{"a":1,"b":[1,2]}"#)) == #"{"a":1,"b":[1,2]}"#)
    }

    @Test func binaryShowsAShortHexRunWithItsSize() {
        let line = PayloadDecoder.preview(Data([0xFF, 0x00, 0x10]))
        #expect(line.hasPrefix("ff 00 10"))
        #expect(line.contains("3"))
        let long = PayloadDecoder.preview(Data(repeating: 0xFE, count: 100))
        #expect(long.contains("…") && long.contains("100"))
    }

    @Test func byteOrderMarkIsSkipped() {
        #expect(PayloadDecoder.preview(bom + data("hi")) == "hi")
    }

    @Test func hugePayloadPreviewIsFast() {
        let huge = Data(repeating: UInt8(ascii: "y"), count: 8 * 1024 * 1024)
        let clock = ContinuousClock()
        var line = ""
        let elapsed = clock.measure { line = PayloadDecoder.preview(huge) }
        #expect(line.utf8.count < PayloadDecoder.previewLimit + 64)
        #expect(elapsed < .seconds(2), "took \(elapsed)")
    }
}
