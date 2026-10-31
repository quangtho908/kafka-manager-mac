// PLACEHOLDER app icon, drawn with CoreGraphics only. Usage: swift tools/make-icon.swift out.png
// Replace it with real artwork (a 1024x1024 PNG fed to scripts/package.sh via ICON_PNG=...) before a public release.
import CoreGraphics
import Foundation
import ImageIO
import UniformTypeIdentifiers

let size = 1024
guard CommandLine.arguments.count == 2,
      let context = CGContext(
        data: nil, width: size, height: size, bitsPerComponent: 8, bytesPerRow: 0,
        space: CGColorSpace(name: CGColorSpace.sRGB)!, bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue)
else {
    FileHandle.standardError.write(Data("usage: make-icon.swift out.png\n".utf8))
    exit(1)
}
let full = CGFloat(size)
// macOS icon grid: the artwork sits inside an 824 pt rounded square with a margin around it.
let tile = CGRect(x: 100, y: 100, width: 824, height: 824)
context.addPath(CGPath(roundedRect: tile, cornerWidth: 185, cornerHeight: 185, transform: nil))
context.clip()
let gradient = CGGradient(
    colorsSpace: CGColorSpace(name: CGColorSpace.sRGB), colors: [
        CGColor(red: 0.13, green: 0.16, blue: 0.24, alpha: 1), CGColor(red: 0.22, green: 0.30, blue: 0.52, alpha: 1),
    ] as CFArray, locations: [0, 1])!
context.drawLinearGradient(gradient, start: CGPoint(x: 0, y: full), end: CGPoint(x: full, y: 0), options: [])
// Three stacked "log" bars with a dot each: a topic's partitions.
context.setFillColor(CGColor(red: 1, green: 1, blue: 1, alpha: 0.95))
for (index, width) in [520.0, 400.0, 300.0].enumerated() {
    let y = 640.0 - Double(index) * 175
    context.addPath(CGPath(roundedRect: CGRect(x: 330, y: y, width: width, height: 90), cornerWidth: 45, cornerHeight: 45, transform: nil))
    context.fillPath()
    context.fillEllipse(in: CGRect(x: 215, y: y + 10, width: 70, height: 70))
}
guard let image = context.makeImage(),
      let destination = CGImageDestinationCreateWithURL(URL(fileURLWithPath: CommandLine.arguments[1]) as CFURL, UTType.png.identifier as CFString, 1, nil)
else { exit(1) }
CGImageDestinationAddImage(destination, image, nil)
exit(CGImageDestinationFinalize(destination) ? 0 : 1)
