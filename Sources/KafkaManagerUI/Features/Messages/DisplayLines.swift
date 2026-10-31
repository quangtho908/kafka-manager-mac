import Foundation

/// One line of payload text as shown in the detail panel.
struct DisplayLine: Identifiable, Sendable, Equatable {
    let id: Int
    let text: String
}

/// Splits payload text into lines for a lazily rendered list, so one 200 KB line never becomes one giant `Text`.
enum DisplayLines {
    /// Characters per displayed line; longer lines continue on the next row.
    static let maxLineLength = 1_000

    /// Lines split at newlines (an empty line stays), then every line cut into pieces of at most `maxLineLength`
    /// characters. Empty text gives no lines.
    static func split(_ text: String, maxLineLength: Int = maxLineLength) -> [DisplayLine] {
        guard !text.isEmpty else { return [] }
        let limit = max(maxLineLength, 1)
        var result: [DisplayLine] = []
        for line in text.split(separator: "\n", omittingEmptySubsequences: false) {
            if line.isEmpty {
                result.append(DisplayLine(id: result.count, text: ""))
                continue
            }
            var rest = line[...]
            while !rest.isEmpty {
                let piece = rest.prefix(limit)
                result.append(DisplayLine(id: result.count, text: String(piece)))
                rest = rest.dropFirst(piece.count)
            }
        }
        return result
    }

    /// True when some line is longer than the limit, so the panel can say it was split.
    static func wasSplit(_ text: String, maxLineLength: Int = maxLineLength) -> Bool {
        let limit = max(maxLineLength, 1)
        return text.split(separator: "\n", omittingEmptySubsequences: false).contains { $0.count > limit }
    }
}
