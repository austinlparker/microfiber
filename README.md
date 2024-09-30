# microfiber

This is a rough POC of a Rust-based Lambda extension layer designed to hook into the Lambda telemetry API and translate it into OTLP.

## Prerequisites

- rust
- cargo lambda

## Build/Deploy

```
cargo lambda build --extension
cargo lambda deploy --extension
```
