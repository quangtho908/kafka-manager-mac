#!/usr/bin/env bash
# Signs dist/KafkaManager.app with a Developer ID, notarizes it with Apple and staples the ticket. FOR THE APP'S OWNER:
# it uses your Apple account, so it is never run by the development tooling.
#
#   scripts/package.sh                                           # builds the app with the default bundle id
#   DEVELOPER_ID_APPLICATION="Developer ID Application: Your Name (TEAMID)" \
#   NOTARY_PROFILE=km-notary \
#   scripts/sign-and-notarize.sh
#
# Required environment:
#   DEVELOPER_ID_APPLICATION  signing identity name (see `security find-identity -v -p codesigning`)
#   NOTARY_PROFILE            a notarytool keychain profile made once with `xcrun notarytool store-credentials`
# Optional (has a default):
#   BUNDLE_ID                 default com.thtsolution.kafkamanager; must equal the one package.sh put in the app
# Optional:
#   DRY_RUN=1   print every command without running it (missing variables are shown as <NAME>)
#   MAKE_DMG=1  also build dist/KafkaManager-<version>.dmg after stapling
# No credential is read or stored by this script: the notary profile lives in your Keychain.
set -euo pipefail
cd "$(dirname "$0")/.."

DRY_RUN="${DRY_RUN:-}"
BUNDLE_ID="${BUNDLE_ID:-com.thtsolution.kafkamanager}"
PLACEHOLDER_ID="dev.kafkamanager.app" # the old default; refused so it is never signed
APP="dist/KafkaManager.app"
ENTITLEMENTS="scripts/KafkaManager.entitlements"
VERSION="$(tr -d '[:space:]' < VERSION)"
ZIP="dist/KafkaManager-$VERSION-notarize.zip"
DMG="dist/KafkaManager-$VERSION.dmg"

missing=()
for name in DEVELOPER_ID_APPLICATION NOTARY_PROFILE; do
  [[ -n "${!name:-}" ]] || missing+=("$name")
done
if ((${#missing[@]})); then
  if [[ -z "$DRY_RUN" ]]; then
    echo "refusing to run: set ${missing[*]} (see the header of this script)" >&2
    exit 2
  fi
  for name in "${missing[@]}"; do export "$name=<$name>"; done
fi

run() {
  echo "+ $*"
  [[ -n "$DRY_RUN" ]] || "$@"
}

if [[ -z "$DRY_RUN" ]]; then
  [[ -d "$APP" ]] || { echo "$APP not found: run scripts/package.sh first" >&2; exit 1; }
  [[ "$BUNDLE_ID" != "$PLACEHOLDER_ID" ]] || { echo "BUNDLE_ID is still the placeholder $PLACEHOLDER_ID: use your own reverse-DNS id" >&2; exit 2; }
  built="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleIdentifier' "$APP/Contents/Info.plist")"
  [[ "$built" == "$BUNDLE_ID" ]] || { echo "the app was built with id '$built', not '$BUNDLE_ID': rerun scripts/package.sh with BUNDLE_ID=$BUNDLE_ID" >&2; exit 2; }
fi

echo "== 1. sign (hardened runtime, secure timestamp, entitlements)"
run codesign --force --options runtime --timestamp --entitlements "$ENTITLEMENTS" --sign "$DEVELOPER_ID_APPLICATION" "$APP"
echo "== 2. verify the signature"
run codesign --verify --strict --deep --verbose=2 "$APP"
echo "== 3. zip for upload"
run rm -f "$ZIP"
run ditto -c -k --keepParent "$APP" "$ZIP"
echo "== 4. notarize (waits for Apple's verdict; a rejection prints with: xcrun notarytool log <submission id> --keychain-profile $NOTARY_PROFILE)"
run xcrun notarytool submit "$ZIP" --keychain-profile "$NOTARY_PROFILE" --wait
echo "== 5. staple the ticket"
run xcrun stapler staple "$APP"
run xcrun stapler validate "$APP"
echo "== 6. Gatekeeper assessment (should say: accepted, source=Notarized Developer ID)"
run spctl -a -t exec -vv "$APP"

if [[ -n "${MAKE_DMG:-}" ]]; then
  echo "== 7. dmg"
  STAGE="dist/dmg-stage"
  run rm -rf "$STAGE" "$DMG"
  run mkdir -p "$STAGE"
  run cp -R "$APP" "$STAGE/"
  run ln -s /Applications "$STAGE/Applications"
  run hdiutil create -volname "Kafka Manager" -srcfolder "$STAGE" -ov -format UDZO "$DMG"
  run codesign --sign "$DEVELOPER_ID_APPLICATION" --timestamp "$DMG"
  run rm -rf "$STAGE"
fi
echo "done."
