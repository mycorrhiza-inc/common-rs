# Features

This crate provides several optional features that can be enabled based on your needs:

## Feature Flags

- `all` - Enables all features
- `s3` - Enables S3 functionality (requires `aws-sdk-s3` and `aws-config`)
- `aide` - Enables API documentation functionality (requires `aide` and `schemars`, also enables `axum`)
- `otel` - Enables OpenTelemetry tracing functionality (requires various `opentelemetry` crates, also enables `axum`)
- `tasks` - Enables task processing functionality (requires `async-trait`, also enables `aide`)
- `llm` - Enables LLM functionality (requires `reqwest`)
- `rkyv` - Enables rkyv serialization functionality (requires `rkyv`)
- `axum` - Enables axum web framework functionality (requires `axum` and `tower-http`)

## Default Features

By default, no features are enabled. You must explicitly enable the features you need.

## Examples

To enable all features:
```toml
[dependencies]
common-rs = { path = "../common-rs", features = ["all"] }
```

To enable specific features:
```toml
[dependencies]
common-rs = { path = "../common-rs", features = ["s3", "aide", "llm"] }
```