#!/usr/bin/env bash
# Builds static OpenSSL + librdkafka into ./vendor (git-ignored). No global installs, no brew.
#   scripts/build-deps.sh                # arm64 (default), idempotent: does nothing when vendor/.stamp matches
#   ARCHS="arm64" scripts/build-deps.sh
#   FORCE=1 scripts/build-deps.sh        # rebuild
#
# Universal (T63, not exercised yet): build once per arch into vendor/<arch>/ (OpenSSL target darwin64-x86_64-cc,
# librdkafka `./configure --host=x86_64-apple-darwin CFLAGS=-arch x86_64`), then `lipo -create` each .a from the
# two trees into vendor/install*/lib. This script only builds the host arch for now.
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
if [[ -z "${FORCE:-}" && -f .stamp && "$(cat .stamp)" == "$STAMP" && -f install/lib/librdkafka.a && -f install-ssl/lib/libssl.a ]]; then
  echo "vendor is up to date ($ARCHS)"
else
  [[ "$ARCHS" == "arm64" ]] || { echo "only ARCHS=arm64 is built here; see the header of this script for x86_64 + lipo" >&2; exit 1; }
  export MACOSX_DEPLOYMENT_TARGET=14.0
  rm -rf install install-ssl openssl-$OPENSSL_VERSION librdkafka .stamp
  [[ -f openssl.tar.gz ]] || curl -sSL -o openssl.tar.gz "https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/openssl-$OPENSSL_VERSION.tar.gz"
  echo "$OPENSSL_SHA256  openssl.tar.gz" | shasum -a 256 -c -
  tar xzf openssl.tar.gz
  (cd openssl-$OPENSSL_VERSION && ./Configure darwin64-arm64-cc no-shared no-tests no-apps no-docs no-legacy no-engine no-dso \
    --prefix="$V/install-ssl" --openssldir="$V/install-ssl/ssl" >/dev/null && make -j8 build_libs >/dev/null && make install_dev >/dev/null)
  git clone -q --depth 1 --branch "$RDKAFKA_VERSION" https://github.com/confluentinc/librdkafka.git
  (cd librdkafka && CPPFLAGS="-I$V/install-ssl/include" LDFLAGS="-L$V/install-ssl/lib" ./configure --prefix="$V/install" \
    --enable-ssl --disable-gssapi --disable-curl --disable-zstd --disable-lz4-ext --enable-static >/dev/null && make -j8 libs >/dev/null && make install >/dev/null)
  rm -f install/lib/*.dylib
  printf '%s\n' "$STAMP" > .stamp
fi
echo "sizes:"; ls -l install/lib/librdkafka.a install-ssl/lib/libssl.a install-ssl/lib/libcrypto.a | awk '{printf "  %7.1f MB  %s\n", $5/1048576, $9}'
