#!/usr/bin/env bash
# Builds static OpenSSL + librdkafka into ./vendor (git-ignored). No global installs, no brew.
#   scripts/build-deps.sh                          # arm64 (default), idempotent: does nothing when vendor/.stamp matches
#   ARCHS="arm64 x86_64" scripts/build-deps.sh     # universal: each arch is built on its own, then lipo'd together
#   FORCE=1 scripts/build-deps.sh                  # rebuild
#
# Layout: vendor/arch-<arch>/{install,install-ssl} hold the per-arch results; vendor/install and vendor/install-ssl are
# what Package.swift links. With several archs their .a files are fat, so one `swift build --arch arm64 --arch x86_64`
# links every slice against the same paths and Package.swift needs no per-arch flags. The other arch is cross-compiled
# with `-arch` (OpenSSL target darwin64-x86_64-cc, librdkafka `--host`); configure tests run under Rosetta if the
# target is x86_64, so Rosetta must already be installed (this script never installs it).
set -euo pipefail
cd "$(dirname "$0")/.."
OPENSSL_VERSION=3.5.9
OPENSSL_SHA256=603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a
RDKAFKA_VERSION=v2.16.0
ARCHS="${ARCHS:-arm64}"
STAMP="openssl=$OPENSSL_VERSION
librdkafka=$RDKAFKA_VERSION
archs=$ARCHS"

mkdir -p vendor; cd vendor; V=$PWD
export MACOSX_DEPLOYMENT_TARGET=14.0

build_arch() {
  local arch="$1" ssl_target host_flag=() A="$V/arch-$1"
  case "$arch" in
    arm64) ssl_target=darwin64-arm64-cc; [[ "$(uname -m)" == arm64 ]] || host_flag=(--host=aarch64-apple-darwin) ;;
    x86_64) ssl_target=darwin64-x86_64-cc; [[ "$(uname -m)" == x86_64 ]] || host_flag=(--host=x86_64-apple-darwin) ;;
    *) echo "unsupported arch $arch" >&2; exit 1 ;;
  esac
  echo "== $arch: OpenSSL $OPENSSL_VERSION"
  rm -rf "$A"; mkdir -p "$A"
  tar xzf openssl.tar.gz -C "$A"
  (cd "$A/openssl-$OPENSSL_VERSION" && ./Configure "$ssl_target" no-shared no-tests no-apps no-docs no-legacy no-engine no-dso \
    --prefix="$A/install-ssl" --openssldir="$A/install-ssl/ssl" >/dev/null && make -j8 build_libs >/dev/null && make install_dev >/dev/null)
  echo "== $arch: librdkafka $RDKAFKA_VERSION"
  git clone -q --depth 1 --branch "$RDKAFKA_VERSION" https://github.com/confluentinc/librdkafka.git "$A/librdkafka"
  (cd "$A/librdkafka" && CC="cc -arch $arch" CXX="c++ -arch $arch" CPPFLAGS="-I$A/install-ssl/include" LDFLAGS="-arch $arch -L$A/install-ssl/lib" \
    ./configure --prefix="$A/install" ${host_flag[@]+"${host_flag[@]}"} \
    --enable-ssl --disable-gssapi --disable-curl --disable-zstd --disable-lz4-ext --enable-static >/dev/null && make -j8 libs >/dev/null && make install >/dev/null)
  rm -f "$A"/install/lib/*.dylib
  rm -rf "$A/openssl-$OPENSSL_VERSION" "$A/librdkafka"
  lipo -info "$A/install/lib/librdkafka.a" "$A/install-ssl/lib/libssl.a" "$A/install-ssl/lib/libcrypto.a"
}

if [[ -z "${FORCE:-}" && -f .stamp && "$(cat .stamp)" == "$STAMP" && -f install/lib/librdkafka.a && -f install-ssl/lib/libssl.a ]]; then
  echo "vendor is up to date ($ARCHS)"
else
  [[ -f openssl.tar.gz ]] || curl -sSL -o openssl.tar.gz "https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/openssl-$OPENSSL_VERSION.tar.gz"
  echo "$OPENSSL_SHA256  openssl.tar.gz" | shasum -a 256 -c -
  for arch in $ARCHS; do build_arch "$arch"; done
  # Only now is the previous install replaced, so a failed build leaves a working vendor/ alone.
  first="${ARCHS%% *}"
  rm -rf install install-ssl .stamp
  cp -R "arch-$first/install" install
  cp -R "arch-$first/install-ssl" install-ssl
  for lib in install/lib/librdkafka.a install-ssl/lib/libssl.a install-ssl/lib/libcrypto.a; do
    inputs=(); for arch in $ARCHS; do inputs+=("arch-$arch/${lib}"); done
    lipo -create "${inputs[@]}" -output "$lib"
  done
  printf '%s\n' "$STAMP" > .stamp
fi
echo "sizes:"; ls -l install/lib/librdkafka.a install-ssl/lib/libssl.a install-ssl/lib/libcrypto.a | awk '{printf "  %7.1f MB  %s\n", $5/1048576, $9}'
lipo -info install/lib/librdkafka.a install-ssl/lib/libssl.a install-ssl/lib/libcrypto.a
