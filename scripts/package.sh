#!/usr/bin/env bash
# Builds the release binary and assembles dist/KafkaManager.app, ad-hoc signed with the hardened runtime.
#   scripts/package.sh
# Environment (all optional):
#   BUNDLE_ID   CFBundleIdentifier. Default com.thtsolution.kafkamanager (the registered App ID).
#   ICON_PNG    1024x1024 PNG for the app icon. Default: resources/AppIcon-1024.png.
#   ARCHS       "arm64" (default) or "arm64 x86_64" for a universal binary (needs vendor built for both: scripts/build-deps.sh).
# The version comes from the VERSION file; the build number is the number of commits on the current branch.
# An ad-hoc signature is NOT accepted by Gatekeeper on other Macs; real distribution is scripts/sign-and-notarize.sh.
set -euo pipefail
cd "$(dirname "$0")/.."

BUNDLE_ID="${BUNDLE_ID:-com.thtsolution.kafkamanager}"
ARCHS="${ARCHS:-arm64}"
VERSION="$(tr -d '[:space:]' < VERSION)"
BUILD="$(git rev-list --count HEAD 2>/dev/null || echo 1)"
APP="dist/KafkaManager.app"
ENTITLEMENTS="scripts/KafkaManager.entitlements"

[[ -f vendor/install/lib/librdkafka.a ]] || { echo "vendor/ is missing: run 'scripts/dev.sh deps' first (the app needs the live Kafka client)" >&2; exit 1; }

ARCH_FLAGS=()
for arch in $ARCHS; do ARCH_FLAGS+=(--arch "$arch"); done
echo "==> swift build -c release ($ARCHS)"
swift build -c release --manifest-cache none "${ARCH_FLAGS[@]}"
BIN_DIR="$(swift build -c release --manifest-cache none "${ARCH_FLAGS[@]}" --show-bin-path)"
BINARY="$BIN_DIR/KafkaManager"
[[ -x "$BINARY" ]] || { echo "missing $BINARY" >&2; exit 1; }

echo "==> assembling $APP (version $VERSION, build $BUILD, id $BUNDLE_ID)"
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp "$BINARY" "$APP/Contents/MacOS/KafkaManager"
printf 'APPL????' > "$APP/Contents/PkgInfo"

# SwiftPM resource bundles (none today) would sit next to the binary; copy them if a target ever adds one.
for bundle in "$BIN_DIR"/*.bundle; do
  [[ -e "$bundle" ]] && cp -R "$bundle" "$APP/Contents/Resources/"
done

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key><string>en</string>
  <key>CFBundleExecutable</key><string>KafkaManager</string>
  <key>CFBundleIconFile</key><string>AppIcon</string>
  <key>CFBundleIdentifier</key><string>${BUNDLE_ID}</string>
  <key>CFBundleInfoDictionaryVersion</key><string>6.0</string>
  <key>CFBundleName</key><string>Kafka Manager</string>
  <key>CFBundleDisplayName</key><string>Kafka Manager</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>${VERSION}</string>
  <key>CFBundleVersion</key><string>${BUILD}</string>
  <key>LSApplicationCategoryType</key><string>public.app-category.developer-tools</string>
  <key>LSMinimumSystemVersion</key><string>14.0</string>
  <key>NSHighResolutionCapable</key><true/>
  <key>NSPrincipalClass</key><string>NSApplication</string>
  <key>NSHumanReadableCopyright</key><string>Copyright © Kafka Manager</string>
</dict>
</plist>
PLIST
plutil -lint "$APP/Contents/Info.plist"

echo "==> icon"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
cp "${ICON_PNG:-resources/AppIcon-1024.png}" "$WORK/icon.png"
ICONSET="$WORK/AppIcon.iconset"; mkdir "$ICONSET"
for size in 16 32 128 256 512; do
  sips -z $size $size "$WORK/icon.png" --out "$ICONSET/icon_${size}x${size}.png" >/dev/null
  sips -z $((size * 2)) $((size * 2)) "$WORK/icon.png" --out "$ICONSET/icon_${size}x${size}@2x.png" >/dev/null
done
iconutil -c icns "$ICONSET" -o "$APP/Contents/Resources/AppIcon.icns"

echo "==> ad-hoc codesign (hardened runtime, entitlements $ENTITLEMENTS)"
codesign --force --options runtime --entitlements "$ENTITLEMENTS" --sign - "$APP"
codesign --verify --strict --deep --verbose=2 "$APP"
codesign -d --entitlements - --verbose=2 "$APP" 2>&1 | grep -E "flags|Identifier|Signature|Authority|TeamIdentifier" || true
echo "==> spctl (an ad-hoc signature is expected to be REJECTED)"
spctl -a -t exec -vv "$APP" 2>&1 || true
echo "==> binary"
lipo -info "$APP/Contents/MacOS/KafkaManager"
echo "done: $APP"
