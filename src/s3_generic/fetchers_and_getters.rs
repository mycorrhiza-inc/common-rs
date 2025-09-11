use anyhow::anyhow;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::types::ObjectCannedAcl;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use futures_util::{StreamExt, stream};
use std::borrow::Cow;
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
        // This is pretty printed just to make it much more readable while debugging objects at the
        // cost of making serialization slower. If this ever becomes a performance bottleneck,
        // switch over to rkyv which should be way way faster than non pretty printed json.
        let obj_json_pretty_string = serde_json::to_string_pretty(obj)?;
        let obj_json_bytes = obj_json_pretty_string.into();
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
                // Match on SDK error to see if it's "NoSuchKey"
                if let SdkError::ServiceError(err) = &e
                    && matches!(err.err(), GetObjectError::NoSuchKey(_))
                {
                    debug!(
                        error = %e,
                        bucket = %self.bucket,
                        key = %self.key,
                        "S3 object not found (NoSuchKey)"
                    );
                    return e; // still return the error, just not as high-level
                }

                let err_dbg = format!("{:?}", e);
                error!(
                    error = %e,
                    error_debug = &err_dbg[..err_dbg.len().min(500)],
                    bucket = %self.bucket,
                    key = %self.key,
                    "Failed to download S3 object"
                );
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
            .acl(ObjectCannedAcl::PublicRead) // 👈 make object public
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

#[derive(Clone)]
pub struct S3DirectoryAddr<'a> {
    pub s3_client: &'a S3Client,
    pub bucket: &'a str,
    pub prefix: Cow<'a, str>,
}

impl<'a> S3DirectoryAddr<'a> {
    pub fn new(s3_client: &'a S3Client, bucket: &'a str, prefix: &'a str) -> Self {
        let actual_prefix = if prefix.ends_with('/') {
            Cow::Borrowed(prefix)
        } else {
            Cow::Owned(format!("{}/", prefix))
        };
        S3DirectoryAddr {
            s3_client,
            bucket,
            prefix: actual_prefix,
        }
    }

    pub async fn delete_all(&self) -> anyhow::Result<()> {
        let mut continuation_token: Option<String> = None;

        loop {
            let mut list_request = self
                .s3_client
                .list_objects_v2()
                .bucket(self.bucket)
                .prefix(&*self.prefix);
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
            .prefix(&*self.prefix)
            .into_paginator()
            .send();

        while let Some(result) = stream.next().await {
            for object in result?.contents() {
                if let Some(key) = object.key() {
                    prefix_names.push(key.to_string());
                }
            }
        }
        Ok(prefix_names)
    }

    /// Copy all files from this prefix to another prefix within the same bucket
    pub async fn copy_into(&self, destination: &S3DirectoryAddr<'_>) -> anyhow::Result<()> {
        // Ensure prefixes end with '/' for proper path handling
        let src_prefix = &self.prefix;

        let dest_prefix = &destination.prefix;

        info!(
            src_bucket = %self.bucket,
            src_prefix = %src_prefix,
            dest_bucket = %destination.bucket,
            dest_prefix = %dest_prefix,
            "Copying files between S3 prefixes"
        );
        let file_list = self.list_all().await?;

        let file_count = stream::iter(file_list)
            .map(|source_key| {
                let s3_client = self.s3_client.clone();
                let bucket = self.bucket.to_string();
                let dest_bucket = destination.bucket.to_string();
                let dest_prefix = destination.prefix.to_string();
                let src_prefix = self.prefix.to_string();

                async move {
                    let relative_path = source_key.strip_prefix(&src_prefix).unwrap_or(&source_key);
                    let destination_key = format!("{}{}", dest_prefix, relative_path);

                    debug!(src_key = %source_key, dest_key = %destination_key, "Copying object");

                    // Perform the copy operation
                    let _copy_res = s3_client
                        .copy_object()
                        .bucket(dest_bucket)
                        .key(&destination_key)
                        .copy_source(format!("{}/{}", bucket, source_key))
                        .send()
                        .await;
                    info!(%destination_key,"Successfully copied file")
                }
            })
            .buffer_unordered(25)
            .count()
            .await;

        info!(
            %file_count,
            src_bucket = %self.bucket,
            src_prefix = %src_prefix,
            dest_bucket = %destination.bucket,
            dest_prefix = %dest_prefix,
            "Successfully copied files between S3 prefixes"
        );

        Ok(())
    }
}
