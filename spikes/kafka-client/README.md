# Kafka client spike (T04) — throwaway, not part of the main package

Proof of concept for librdkafka v2.16.0 (static, with OpenSSL 3.5.9) called directly from Swift via a system-library module.

```sh
cd spikes/kafka-client
./build-deps.sh                      # ~5 min; fetches OpenSSL + librdkafka sources into vendor/ (git-ignored)
swift build -c release
.build/release/poc --seed 60000      # create spike-orders (3 partitions) + 60000 x 2KB messages (separate process)
.build/release/poc --skip-seed --keep --linger 3000                       # default buffers
.build/release/poc --skip-seed --keep --linger 3000 --tuned --prop fetch.queue.backoff.ms=50   # tuned buffers
.build/release/poc --ssl-smoke       # sasl_ssl PLAIN/SCRAM clients can be created (no live handshake)
.build/release/poc --snapshot        # groups + committed offsets + topics (compare before/after)
.build/release/poc --cleanup         # delete spike-g and spike-orders
```
Needs a broker on localhost:9092 (`--bootstrap host:port` to change). Flags: `--legacy` simple-consumer API, `--no-warmup`, `--prop k=v`.
Measured output of the runs is in `results/`.
