#!/usr/bin/env bash
# Builds static OpenSSL 3.5.9 and librdkafka v2.16.0 into ./vendor (no global installs). arm64, macOS 14 minimum.
set -euo pipefail
cd "$(dirname "$0")"; mkdir -p vendor; cd vendor; V=$PWD
export MACOSX_DEPLOYMENT_TARGET=14.0
curl -sSL -o openssl.tar.gz https://github.com/openssl/openssl/releases/download/openssl-3.5.9/openssl-3.5.9.tar.gz
echo "603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a  openssl.tar.gz" | shasum -a 256 -c -
tar xzf openssl.tar.gz && (cd openssl-3.5.9 && ./Configure darwin64-arm64-cc no-shared no-tests no-apps no-docs no-legacy no-engine no-dso \
  --prefix="$V/install-ssl" --openssldir="$V/install-ssl/ssl" >/dev/null && make -j8 build_libs >/dev/null && make install_dev >/dev/null)
git clone -q --depth 1 --branch v2.16.0 https://github.com/confluentinc/librdkafka.git
(cd librdkafka && CPPFLAGS="-I$V/install-ssl/include" LDFLAGS="-L$V/install-ssl/lib" ./configure --prefix="$V/install" \
  --enable-ssl --disable-gssapi --disable-curl --disable-zstd --disable-lz4-ext --enable-static >/dev/null && make -j8 libs >/dev/null && make install >/dev/null)
