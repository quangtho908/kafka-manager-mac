import SwiftUI

private struct FindActionKey: FocusedValueKey {
    typealias Value = () -> Void
}

public extension FocusedValues {
    /// Set by a screen that has a search field; the Find menu item is disabled while it is nil.
    var findAction: (() -> Void)? {
        get { self[FindActionKey.self] }
        set { self[FindActionKey.self] = newValue }
    }
}
