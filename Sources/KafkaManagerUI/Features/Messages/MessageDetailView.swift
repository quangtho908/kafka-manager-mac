import SwiftUI
import KafkaManagerCore

/// Key, Value, Headers and Metadata of the selected message. The parent gives it `.id(messageID)`, so each selection
/// gets a fresh model and nothing from the previous message can show.
struct MessageDetailView: View {
    @State private var model: MessageDetailViewModel
    @State private var copyKind: MessageCopyKind?
    @State private var copyRequest = 0
    private let produceAgain: ProduceAgainControl?

    private enum Metrics {
        static let padding: Double = 12
        static let spacing: Double = 8
        static let codeSpacing: Double = 1
        static let minHeight: Double = 140
        static let labelWidth: Double = 90
    }

    init(topic: String, message: KafkaMessage, clipboard: any Clipboard, produceAgain: ProduceAgainControl? = nil) {
        self.produceAgain = produceAgain
        _model = State(initialValue: MessageDetailViewModel(topic: topic, message: message, clipboard: clipboard))
    }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: Metrics.padding) {
                header
                PayloadSection(title: "Key", part: .key, model: model)
                PayloadSection(title: "Value", part: .value, model: model)
                if !model.message.headers.isEmpty { headers }
                metadata
            }
            .padding(Metrics.padding)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .frame(minHeight: Metrics.minHeight)
        .task(id: model.keyFormat) { await model.load(.key) }
        .task(id: model.valueFormat) { await model.load(.value) }
        .task(id: copyRequest) {
            guard let kind = copyKind else { return }
            switch kind {
            case .key: await model.copy(.key)
            case .value: await model.copy(.value)
            case .message: model.copyMessage()
            }
        }
    }

    private var header: some View {
        HStack(spacing: Metrics.spacing) {
            Text("Message \(model.message.partition):\(model.message.offset)")
                .font(.headline)
            Spacer()
            if let copied = model.lastCopy {
                Label(copiedText(copied), systemImage: "checkmark.circle").font(.callout).foregroundStyle(.green)
            }
            Toggle("Wrap lines", isOn: $model.wordWrap).toggleStyle(.checkbox).controlSize(.small)
            Button("Copy Key (as shown)") { request(.key) }
                .help(Text("Copies the whole key in the format chosen above"))
                .disabled(!model.canCopy(.key) || model.isCopying)
            Button("Copy Value (as shown)") { request(.value) }
                .help(Text("Copies the whole value in the format chosen above"))
                .disabled(!model.canCopy(.value) || model.isCopying)
            Button("Copy Message") { request(.message) }
                .help(Text("Copies the message as JSON"))
            if let produceAgain {
                Button("Produce Again…") { produceAgain.open(model.message) }
                    .help(Text(produceAgain.gate.disabledReason ?? String(localized: "Send a copy of this message to the topic")))
                    .disabled(!produceAgain.gate.allowsWrites)
            }
        }
    }

    private func request(_ kind: MessageCopyKind) {
        copyKind = kind
        copyRequest += 1
    }

    private func copiedText(_ kind: MessageCopyKind) -> String {
        switch kind {
        case .key: String(localized: "Key copied")
        case .value: String(localized: "Value copied")
        case .message: String(localized: "Message copied")
        }
    }

    private var headers: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing / 2) {
            Text("Headers").font(.subheadline.weight(.semibold))
            ForEach(model.headerRows) { row in
                HStack(alignment: .firstTextBaseline, spacing: Metrics.spacing) {
                    Text(row.key).font(.callout.weight(.medium)).frame(width: Metrics.labelWidth * 1.4, alignment: .leading)
                    Text(row.value).font(.system(.callout, design: .monospaced)).lineLimit(3)
                    Spacer(minLength: 0)
                }
                .textSelection(.enabled)
            }
        }
    }

    private var metadata: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing / 2) {
            Text("Metadata").font(.subheadline.weight(.semibold))
            ForEach(model.metadataRows) { row in
                HStack(alignment: .firstTextBaseline, spacing: Metrics.spacing) {
                    Text(row.label).foregroundStyle(.secondary).frame(width: Metrics.labelWidth, alignment: .leading)
                    Text(row.value).monospacedDigit()
                    Spacer(minLength: 0)
                }
                .font(.callout)
                .textSelection(.enabled)
            }
        }
    }
}

/// One of Key / Value: format picker, notes, and the lazily rendered text.
private struct PayloadSection: View {
    let title: LocalizedStringKey
    let part: MessageDetailPart
    @Bindable var model: MessageDetailViewModel

    private enum Metrics {
        static let spacing: Double = 6
        static let maxHeight: Double = 260
        static let padding: Double = 6
        static let corner: Double = 6
        static let pickerWidth: Double = 260
    }

    private var view: PayloadView { part == .key ? model.key : model.value }

    var body: some View {
        VStack(alignment: .leading, spacing: Metrics.spacing) {
            HStack {
                Text(title).font(.subheadline.weight(.semibold))
                Picker("Format", selection: format) {
                    Text("Auto").tag(PayloadFormat.auto)
                    Text("JSON").tag(PayloadFormat.json)
                    Text("Text").tag(PayloadFormat.text)
                    Text("Hex").tag(PayloadFormat.hex)
                }
                .pickerStyle(.segmented)
                .labelsHidden()
                .frame(maxWidth: Metrics.pickerWidth)
                .accessibilityLabel(Text("\(Text(title)) format"))
                if view.isDecoding { ProgressView().controlSize(.small).accessibilityLabel(Text("Decoding")) }
            }
            ForEach(notes, id: \.self) { note in
                Label(note, systemImage: "info.circle").font(.callout).foregroundStyle(.secondary)
            }
            content
        }
    }

    private var format: Binding<PayloadFormat> {
        Binding(get: { model.format(of: part) }, set: { model.setFormat($0, for: part) })
    }

    private var notes: [String] {
        var result = view.payload?.notes ?? []
        if view.wasSplit {
            result.append(String(localized: "Lines longer than \(DisplayLines.maxLineLength) characters continue on the next row."))
        }
        return result
    }

    @ViewBuilder private var content: some View {
        if let payload = view.payload, payload.kind == .null || payload.kind == .empty {
            Text(payload.displayText).foregroundStyle(.secondary).italic()
        } else if view.lines.isEmpty {
            if !view.isDecoding { Color.clear.frame(height: 1) }
        } else {
            lines
        }
    }

    @ViewBuilder private var lines: some View {
        let list = LazyVStack(alignment: .leading, spacing: 0) {
            ForEach(view.lines) { line in
                if model.wordWrap {
                    Text(line.text.isEmpty ? " " : line.text)
                        .frame(maxWidth: .infinity, alignment: .leading)
                } else {
                    Text(line.text.isEmpty ? " " : line.text).fixedSize(horizontal: true, vertical: false)
                }
            }
        }
        .font(.system(.callout, design: .monospaced))
        .textSelection(.enabled)
        .padding(Metrics.padding)

        Group {
            if model.wordWrap {
                ScrollView(.vertical) { list }
            } else {
                ScrollView([.vertical, .horizontal]) { list }
            }
        }
        .frame(maxHeight: Metrics.maxHeight)
        .background(.quaternary.opacity(0.4), in: RoundedRectangle(cornerRadius: Metrics.corner))
    }
}
