use anyhow::anyhow;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use std::path::Path;
use tracing::{debug, error, info};

// Conditional imports for rkyv
#[cfg(feature = "rkyv")]
use rkyv::api::high::{HighSerializer, HighValidator};
#[cfg(feature = "rkyv")]
use rkyv::bytecheck::CheckBytes;
#[cfg(feature = "rkyv")]
use rkyv::de::Pool;
#[cfg(feature = "rkyv")]
use rkyv::rancor::Strategy;
#[cfg(feature = "rkyv")]
use rkyv::ser::allocator::ArenaHandle;
#[cfg(feature = "rkyv")]
use rkyv::util::AlignedVec;
#[cfg(feature = "rkyv")]
use rkyv::{Archive, Serialize};

#[derive(Clone, Copy)]
pub struct S3Addr<'a> {
    pub s3_client: &'a S3Client,
    pub bucket: &'a str,
    pub key: &'a str,
}

impl<'a> S3Addr<'a> {
    pub fn new(s3_client: &'a S3Client, bucket: &'a str, key: &'a str) -> Self {
        S3Addr {
            s3_client,
            bucket,
            key,
        }
    }

    pub async fn download_json<T: serde::de::DeserializeOwned>(&self) -> anyhow::Result<T> {
        let bytes = self.download_bytes().await?;
        let case = serde_json::from_slice(&bytes)?;
        Ok(case)
    }

    pub async fn upload_json<T: serde::Serialize>(&self, obj: &T) -> anyhow::Result<()> {
        let obj_json_bytes = serde_json::to_vec(obj)?;
        self.upload_bytes(obj_json_bytes).await
    }

    #[cfg(feature = "rkyv")]
    pub async fn download_rkyv<T>(&self) -> anyhow::Result<T>
    where
        T: Archive,
        T::Archived: for<'b> CheckBytes<HighValidator<'b, rkyv::rancor::Error>>
            + rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        let bytes = self.download_bytes().await?;
        let value = rkyv::from_bytes(&bytes)?;
        Ok(value)
    }

    #[cfg(feature = "rkyv")]
    pub async fn upload_rkyv<T>(&self, obj: &T) -> anyhow::Result<()>
    where
        T: Archive
            + for<'b> Serialize<HighSerializer<AlignedVec, ArenaHandle<'b>, rkyv::rancor::Error>>,
    {
        let bytes = rkyv::to_bytes(obj)?;
        self.upload_bytes(bytes.to_vec()).await
    }

    pub async fn download_bytes(&self) -> anyhow::Result<Vec<u8>> {
        debug!(%self.bucket, %self.key,"Downloading S3 object");
        let output = self
            .s3_client
            .get_object()
            .bucket(self.bucket)
            .key(self.key)
            .send()
            .await
            .map_err(|e| {
                error!(error = %e, %self.bucket, %self.key,"Failed to download S3 object");
                e
            })?;

        let bytes = output
            .body
            .collect()
            .await
            .map(|data| data.into_bytes().to_vec())
            .map_err(|e| {
                error!(error = %e,%self.bucket, %self.key, "Failed to read response body");
                e
            })?;

        debug!(
            %self.bucket,
            %self.key,
            bytes_len = %bytes.len(),
            "Successfully downloaded file from s3"
        );
        Ok(bytes)
    }

    pub async fn upload_bytes(&self, bytes: Vec<u8>) -> anyhow::Result<()> {
        debug!(len=%bytes.len(), %self.bucket, %self.key,"Uploading bytes to S3 object");
        self.s3_client
            .put_object()
            .bucket(self.bucket)
            .key(self.key)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|err| {
                error!(%err,%self.bucket, %self.key,"Failed to upload S3 object");
                anyhow!(err)
            })?;
        debug!( %self.bucket, %self.key,"Successfully uploaded s3 object");
        Ok(())
    }

    pub async fn delete_file(&self) -> anyhow::Result<()> {
        debug!( %self.bucket, %self.key,"Deleting file from S3");
        self.s3_client
            .delete_object()
            .bucket(self.bucket)
            .key(self.key)
            .send()
            .await
            .map_err(|err| {
                error!(%err,%self.bucket, %self.key,"Failed to delete s3 file");
                anyhow!(err)
            })?;
        debug!( %self.bucket, %self.key,"Successfully deleted s3 file");
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct S3PrefixAddr<'a> {
    pub s3_client: &'a S3Client,
    pub bucket: &'a str,
    pub prefix: &'a str,
}

impl<'a> S3PrefixAddr<'a> {
    pub fn new(s3_client: &'a S3Client, bucket: &'a str, prefix: &'a str) -> Self {
        S3PrefixAddr {
            s3_client,
            bucket,
            prefix,
        }
    }

    pub async fn delete_all(&self) -> anyhow::Result<()> {
        let mut continuation_token: Option<String> = None;

        loop {
            let mut list_request = self
                .s3_client
                .list_objects_v2()
                .bucket(self.bucket)
                .prefix(self.prefix);
            if let Some(token) = continuation_token {
                list_request = list_request.continuation_token(token);
            }
            let response = list_request.send().await?;
            if let Some(objects) = response.contents {
                for object in objects {
                    if let Some(key) = object.key {
                        S3Addr::new(self.s3_client, self.bucket, &key)
                            .delete_file()
                            .await?;
                    }
                }
            }
            match response.is_truncated {
                Some(true) => continuation_token = response.next_continuation_token,
                _ => break,
            }
        }
        Ok(())
    }

    pub async fn list_all(&self) -> anyhow::Result<Vec<String>> {
        let mut prefix_names = Vec::new();

        let mut stream = self
            .s3_client
            .list_objects_v2()
            .bucket(self.bucket)
            .prefix(self.prefix)
            .into_paginator()
            .send();

        while let Some(result) = stream.next().await {
            for object in result?.contents() {
                if let Some(key) = object.key() {
                    info!(%key, "Found list attachment object");
                    if key.ends_with(".json")
                        && let Some(filename) = Path::new(key).file_name()
                        && let Some(filestem) = filename.to_str()
                    {
                        prefix_names.push(filestem.to_string());
                    }
                }
            }
        }
        Ok(prefix_names)
    }
}
