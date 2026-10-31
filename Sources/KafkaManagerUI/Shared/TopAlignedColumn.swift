import SwiftUI

extension View {
    /// Every column or tab root uses this: the content fills the column and starts at the top. Without it, a stack
    /// whose children don't expand (an empty state, a placeholder row) is centred vertically in the column.
    /// Pure placeholders (`ContentUnavailableView`) are the only things that should centre, and they do so inside
    /// the space this leaves them.
    func topAlignedColumn() -> some View {
        frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    }
}
