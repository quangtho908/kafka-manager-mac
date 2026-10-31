import Testing

/// Parent of every suite that touches `ResourceCounters`: nested suites of a serialized suite run one after another,
/// so the counts they assert are not disturbed by the others.
@Suite(.serialized)
enum LiveSerial {}
