# Vendored zstd for NanoMQ

This directory contains the vendored `zstd` source used by NanoMQ TAOS builds.

Compatibility policy:

- NanoMQ TAOS builds prefer this vendored copy by default.
- If `-DNANOMQ_USE_BUNDLED_ZSTD=OFF` is set, NanoMQ falls back to:
  - `-DNANOMQ_ZSTD_INCLUDE_DIR=...`
  - `-DNANOMQ_ZSTD_LIBRARY=...`
  - system `find_path()` / `find_library()`
- `ENABLE_TAOS=ON` requires both zstd headers and library. Missing zstd is a hard error.

Vendored contents:

- `build/cmake/`
- `lib/`
- upstream `LICENSE`, `COPYING`, `README.md`

Current upstream baseline:

- zstd 1.6.0

Update workflow:

1. Refresh the vendored source from the approved upstream/local mirror.
2. Keep the vendored layout stable: `build/cmake` and `lib` must remain present.
3. Re-run a TAOS-enabled NanoMQ configure/build on Windows and Linux.
4. Confirm weld telemetry zstd decoding still links and runs.
