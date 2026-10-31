import Foundation
import Observation
import KafkaManagerCore

/// What the user picks in the reset sheet.
public enum OffsetTargetKind: Hashable, Sendable, CaseIterable, Identifiable {
    case latest, earliest, time, offset

    public var id: Self { self }

    public var title: String {
        switch self {
        case .latest: String(localized: "Latest (skip everything)")
        case .earliest: String(localized: "Earliest (read it all again)")
        case .time: String(localized: "A point in time")
        case .offset: String(localized: "A specific offset")
        }
    }
}

/// Resetting one group's committed offsets: choose a target and scope, see exactly what would change, then apply after a
/// typed confirmation. Nothing is written before `confirmApply`. Latest, earliest and offset targets are planned locally
/// from one read of the group; only a point in time asks the broker again.
@MainActor @Observable
public final class OffsetResetViewModel: Identifiable {
    public nonisolated let id = UUID()
    public let groupId: String

    public var targetKind: OffsetTargetKind
    public var time: Date
    public var offsetText = ""
    /// When false every topic the group uses is reset; when true only `selectedTopics`.
    public var onlySelectedTopics = false
    public var selectedTopics: Set<String> = []

    public private(set) var preview = LoadState<OffsetResetPreview>.idle
    /// True while a preview reloads over one that is already showing.
    public private(set) var isRefreshing = false
    public private(set) var isApplying = false
    /// The broker's answer to the last apply, partial failures included.
    public private(set) var result: OffsetResetResult?
    /// Lag totals around the last apply, for the success note.
    public private(set) var appliedLag: (before: Int64, after: Int64)?
    public private(set) var applyError: String?
    /// The typed confirmation awaiting the user; the view presents it as a sheet.
    public var pendingConfirmation: DestructiveConfirmation?

    @ObservationIgnored private let session: ClusterSession
    @ObservationIgnored private let openingState: GroupState
    @ObservationIgnored private let onApplied: @MainActor () async -> Void
    @ObservationIgnored private var base: OffsetResetPreview?
    @ObservationIgnored private var requestID = 0
    @ObservationIgnored private var applyTask: Task<Void, Never>?

    public init(
        session: ClusterSession, groupId: String, groupState: GroupState, target: OffsetTargetKind,
        now: Date = Date(), onApplied: @escaping @MainActor () async -> Void = {}
    ) {
        self.session = session
        self.groupId = groupId
        self.openingState = groupState
        self.targetKind = target
        self.time = now.addingTimeInterval(-3600)
        self.onApplied = onApplied
    }

    // MARK: Options

    /// The chosen target, or nil while the offset field does not hold a number.
    public var target: OffsetResetTarget? {
        switch targetKind {
        case .latest: .latest
        case .earliest: .earliest
        case .time: .timestamp(time)
        case .offset: Int64(offsetText.trimmingCharacters(in: .whitespaces)).flatMap { $0 >= 0 ? OffsetResetTarget.offset($0) : nil }
        }
    }

    public var targetIssue: String? {
        guard targetKind == .offset, target == nil else { return nil }
        return String(localized: "Enter an offset: a whole number, 0 or more.")
    }

    /// What the preview depends on; the view reloads when it changes.
    public struct PreviewKey: Hashable, Sendable {
        let kind: OffsetTargetKind
        let time: Date?
        let offset: Int64?
    }

    public var previewKey: PreviewKey {
        var offset: Int64?
        if case .offset(let value)? = target { offset = value }
        return PreviewKey(kind: targetKind, time: targetKind == .time ? time : nil, offset: offset)
    }

    public var availableTopics: [String] {
        Array(Set((base ?? preview.value)?.rows.map(\.topic) ?? [])).sorted()
    }

    /// The preview limited to the topics in scope.
    public var scopedPreview: OffsetResetPreview? {
        guard let preview = preview.value else { return nil }
        return onlySelectedTopics ? preview.limited(to: .topics(selectedTopics)) : preview
    }

    // MARK: Preview

    /// Loads what the chosen options would do. The latest request wins; a cancelled caller changes nothing.
    public func refreshPreview() async {
        requestID += 1
        let id = requestID
        guard let target else { return }
        if preview.value == nil { preview = .loading } else { isRefreshing = true }
        let result: Result<OffsetResetPreview, any Error>
        do {
            result = .success(try await planned(for: target))
        } catch {
            result = .failure(error)
        }
        guard id == requestID, !Task.isCancelled else { return }
        isRefreshing = false
        switch result {
        case .success(let value):
            if preview.value == nil { selectedTopics = Set(value.rows.map(\.topic)) }
            preview = .loaded(value)
        case .failure(let error):
            preview = .failed(message: Self.message(for: error))
        }
    }

    /// Reads the group again (after an apply, or when the user asks).
    public func reload() async {
        base = nil
        await refreshPreview()
    }

    private func planned(for target: OffsetResetTarget) async throws -> OffsetResetPreview {
        let client = try await session.ensureConnected()
        if case .timestamp = target {
            return try await client.previewOffsetReset(group: groupId, target: target, scope: .wholeGroup)
        }
        if base == nil {
            base = try await client.previewOffsetReset(group: groupId, target: .latest, scope: .wholeGroup)
        }
        guard let base else { throw CancellationError() }
        // The base rows hold everything the planner needs: committed offset and the retained range.
        let lags = base.rows.map {
            PartitionLag(topic: $0.topic, partition: $0.partition, committedOffset: $0.currentCommitted, startOffset: $0.start, endOffset: $0.end)
        }
        return OffsetResetPreview(group: base.group, groupState: base.groupState, rows: OffsetResetPlanner.rows(lags: lags, target: target))
    }

    // MARK: Apply

    public var writeGate: WriteGate { WriteGate(isReadOnly: session.isReadOnly) }

    /// Why Apply is not offered; nil when it is.
    public var applyDisabledReason: String? {
        if let reason = writeGate.disabledReason { return reason }
        if let refusal = OffsetResetRule.refusal(for: preview.value?.groupState ?? openingState) { return refusal }
        if let issue = targetIssue { return issue }
        guard let scoped = scopedPreview else { return String(localized: "Waiting for the preview") }
        if scoped.changedRows.isEmpty { return String(localized: "Nothing would change") }
        if isApplying { return String(localized: "Applying…") }
        return nil
    }

    /// Opens the typed confirmation. Nothing is written yet.
    public func requestApply() {
        guard applyDisabledReason == nil, let scoped = scopedPreview else { return }
        applyError = nil
        result = nil
        let count = scoped.changedRows.count
        let partitions = count == 1 ? String(localized: "1 partition") : String(localized: "\(count) partitions")
        pendingConfirmation = DestructiveConfirmation(
            title: String(localized: "Reset offsets?"),
            clusterName: session.connection.name,
            environment: session.connection.environment,
            objectKind: String(localized: "consumer group"),
            objectName: groupId,
            actionTitle: String(localized: "Reset Offsets"),
            consequences: String(localized: "The committed offsets of \(partitions) on \(session.connection.name) move to the new positions. Consumers will resume from there; skipped messages are not deleted but this group will not process them. This cannot be undone."))
    }

    /// Called by the confirmation sheet. The apply is owned by this model, so closing a sheet does not cancel a request
    /// that is already on its way.
    public func confirmApply() {
        guard applyTask == nil else { return }
        applyTask = Task { [weak self] in
            await self?.apply()
            self?.applyTask = nil
        }
    }

    public func apply() async {
        guard !isApplying, applyDisabledReason == nil, let scoped = scopedPreview else { return }
        isApplying = true
        applyError = nil
        result = nil
        let plan = scoped.plan
        let lag = (scoped.totalLagBefore, scoped.totalLagAfter)
        do {
            let client = try await session.ensureConnected()
            let outcome = try await client.resetOffsets(group: groupId, plan: plan)
            result = outcome
            if outcome.applied > 0 { appliedLag = (lag.0, lag.1) }
            isApplying = false
            await onApplied()
            await reload()
        } catch {
            isApplying = false
            applyError = Self.message(for: error)
            // A refusal usually means the group changed under us: show its real state.
            if (error as? KafkaError)?.isNonEmptyGroup == true { await reload() }
        }
    }

    /// "Reset 6 partitions. Lag 1,234 → 0." after a fully successful apply.
    public var successNote: String? {
        guard let result, result.isComplete, result.applied > 0 else { return nil }
        let partitions = result.applied == 1 ? String(localized: "1 partition") : String(localized: "\(result.applied) partitions")
        guard let lag = appliedLag else { return String(localized: "Reset \(partitions).") }
        return String(localized: "Reset \(partitions). Lag \(lag.before.formatted()) → \(lag.after.formatted()).")
    }

    private static func message(for error: any Error) -> String {
        switch error {
        case let error as KafkaError: error.userMessage
        case let error as ClusterSessionError: error.userMessage
        default: String(localized: "Couldn't reset the offsets.")
        }
    }
}
