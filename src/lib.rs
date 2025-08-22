#![allow(dead_code)]

// Always active modules
pub mod file_extension;
pub mod hash;
pub mod misc;

// Conditionally compiled modules
#[cfg(feature = "aide")]
pub mod api_documentation;

#[cfg(feature = "llm")]
pub mod llm_deepinfra;

#[cfg(feature = "otel")]
pub mod otel_tracing;

#[cfg(feature = "s3")]
pub mod s3_generic;

#[cfg(feature = "tasks")]
pub mod tasks;
