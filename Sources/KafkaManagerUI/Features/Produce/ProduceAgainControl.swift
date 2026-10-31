import KafkaManagerCore

/// What the messages views need to offer "Produce Again…": whether writes are allowed, and how to open the sheet.
struct ProduceAgainControl {
    let gate: WriteGate
    let open: (KafkaMessage) -> Void
}
