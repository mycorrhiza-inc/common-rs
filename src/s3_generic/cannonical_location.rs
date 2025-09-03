use aws_sdk_s3::Client;

use crate::s3_generic::{
    S3Credentials, fetchers_and_getters::S3Addr, s3_uri::S3LocationWithCredentials,
};

pub trait CannonicalS3ObjectLocation: serde::Serialize + serde::de::DeserializeOwned {
    type AddressInfo;
    fn generate_object_key(addr: &Self::AddressInfo) -> String;
    fn generate_bucket(addr: &Self::AddressInfo) -> &'static str;
    fn get_credentials(addr: &Self::AddressInfo) -> &'static S3Credentials;
}

pub fn get_openscrapers_json_key<T: CannonicalS3ObjectLocation>(addr: &T::AddressInfo) -> String {
    T::generate_object_key(addr) + ".json"
}

pub fn get_s3_json_uri<T: CannonicalS3ObjectLocation>(addr: &T::AddressInfo) -> String {
    let bucket = T::generate_bucket(&addr);
    let key = get_openscrapers_json_key::<T>(addr);
    let credentials = T::get_credentials(addr);
    S3LocationWithCredentials::from_key_bucket_and_credentials(&key, bucket, credentials)
        .to_string()
}

pub async fn download_openscrapers_object<T: CannonicalS3ObjectLocation>(
    s3_client: &Client,
    addr: &T::AddressInfo,
) -> anyhow::Result<T> {
    let key = get_openscrapers_json_key::<T>(addr);
    let bucket = T::generate_bucket(&addr);
    S3Addr::new(s3_client, bucket, &key).download_json().await
}

pub async fn upload_object<T: CannonicalS3ObjectLocation>(
    s3_client: &Client,
    addr: &T::AddressInfo,
    object: &T,
) -> anyhow::Result<()> {
    let key = get_openscrapers_json_key::<T>(addr);
    let bucket = T::generate_bucket(&addr);
    S3Addr::new(s3_client, bucket, &key)
        .upload_json(&object)
        .await
}

pub async fn delete_openscrapers_s3_object<T: CannonicalS3ObjectLocation>(
    s3_client: &Client,
    addr: &T::AddressInfo,
) -> anyhow::Result<()> {
    let key = get_openscrapers_json_key::<T>(addr);
    let bucket = T::generate_bucket(&addr);
    S3Addr::new(s3_client, bucket, &key).delete_file().await
}
