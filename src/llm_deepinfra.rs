use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::Debug;
use std::sync::LazyLock;

use crate::misc::fmap_empty;

pub static DEEPINFRA_API_KEY: LazyLock<String> =
    LazyLock::new(|| env::var("DEEPINFRA_API_KEY").expect("Expected DEEPINFRA_API_KEY"));

pub const FAST_CHEAP_MODEL_NAME: &str = "meta-llama/Llama-4-Maverick-17B-128E-Instruct-Turbo";
pub const REASONING_MODEL_NAME: &str = "meta-llama/Meta-Llama-3-70B-Instruct";

#[derive(Debug, thiserror::Error)]
pub enum DeepInfraError {
    #[error("HTTP request failed: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Failed to deserialize response: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("API returned an error: {0}")]
    ApiError(String),
    #[error("No choices returned from API")]
    NoChoices,
}

#[derive(Serialize)]
struct DeepInfraRequestBody {
    model: &'static str,
    messages: Vec<DeepInfraMessage>,
}

#[derive(Serialize, Deserialize)]
struct DeepInfraMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct DeepInfraResponseBody {
    choices: Vec<DeepInfraChoice>,
    usage: DeepInfraResponseUsage,
}

#[derive(Deserialize)]
struct DeepInfraChoice {
    message: DeepInfraMessage,
}

#[derive(Deserialize)]
struct DeepInfraResponseUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

async fn simple_prompt(
    model_name: &'static str,
    system_prompt: Option<&str>,
    user_prompt: Option<&str>,
) -> Result<String, DeepInfraError> {
    let client = reqwest::Client::new();

    let mut messages = Vec::new();
    if let Some(sys_prompt) = fmap_empty(system_prompt) {
        messages.push(DeepInfraMessage {
            role: "system".into(),
            content: sys_prompt.into(),
        });
    }
    if let Some(usr_prompt) = fmap_empty(user_prompt) {
        messages.push(DeepInfraMessage {
            role: "user".into(),
            content: usr_prompt.into(),
        });
    }

    let request_body = DeepInfraRequestBody {
        model: model_name,
        messages,
    };

    let response = client
        .post("https://api.deepinfra.com/v1/openai/chat/completions")
        .header("Authorization", format!("Bearer {}", *DEEPINFRA_API_KEY))
        .json(&request_body)
        .send()
        .await?;

    if !response.status().is_success() {
        let error_body = response.text().await?;
        return Err(DeepInfraError::ApiError(error_body));
    }

    let response_body: DeepInfraResponseBody = response.json().await?;

    if let Some(choice) = response_body.choices.into_iter().next() {
        Ok(choice.message.content.to_string())
    } else {
        Err(DeepInfraError::NoChoices)
    }
}

pub async fn cheap_prompt(sys_prompt: &str) -> Result<String, DeepInfraError> {
    simple_prompt(FAST_CHEAP_MODEL_NAME, Some(sys_prompt), None).await
}

pub async fn reasoning_prompt(sys_prompt: &str) -> Result<String, DeepInfraError> {
    simple_prompt(REASONING_MODEL_NAME, Some(sys_prompt), None).await
}

pub fn strip_think(input: &str) -> &str {
    input.split("</think>").last().unwrap_or(input).trim()
}

pub async fn test_deepinfra() -> Result<String, String> {
    cheap_prompt("What is your favorite color?")
        .await
        .map_err(|e| e.to_string())
}
