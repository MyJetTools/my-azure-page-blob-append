use std::time::Duration;

use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

pub async fn create_container_if_not_exist<TMyPageBlob: MyPageBlob>(
    my_page_blob: &mut TMyPageBlob,
) -> Result<(), AzureStorageError> {
    let mut attemt_no = 0;
    loop {
        attemt_no += 1;
        let result = my_page_blob.create_container_if_not_exist().await;

        if result.is_ok() {
            return Ok(());
        }

        let err = result.err().unwrap();

        match &err {
            my_azure_storage_sdk::AzureStorageError::HyperError { err } => {
                println!(
                    "We have problem on HTTP Level. Attempt: {} Err: {:?}",
                    attemt_no, err
                );
            }
            _ => return Err(err),
        }
    }
}

pub async fn create_blob_if_not_exists<TMyPageBlob: MyPageBlob>(
    my_page_blob: &mut TMyPageBlob,
    init_page_size: usize,
) -> Result<(), AzureStorageError> {
    let mut attemt_no = 0;
    loop {
        attemt_no += 1;
        let result = my_page_blob.create_if_not_exists(init_page_size).await;

        if result.is_ok() {
            return Ok(());
        }

        let err = result.err().unwrap();

        match &err {
            my_azure_storage_sdk::AzureStorageError::HyperError { err } => {
                println!(
                    "We have problem on HTTP Level. Attempt: {} Err: {:?}",
                    attemt_no, err
                );
            }
            _ => return Err(err),
        }
    }
}

pub async fn get_available_pages_amount<TMyPageBlob: MyPageBlob>(
    page_blob: &mut TMyPageBlob,
) -> Result<usize, AzureStorageError> {
    let mut attempt_no = 1;

    loop {
        let result = page_blob.get_available_pages_amount().await;

        if result.is_ok() {
            return result;
        }

        let err = result.err().unwrap();

        match &err {
            AzureStorageError::ContainerNotFound => {
                crate::page_blob_utils::create_container_with_retires(page_blob).await?;
            }
            AzureStorageError::HyperError { err: _ } => {
                println!(
                    "Can not execute get_available_pages_amount because of  {:?}. Attempt {} Retrying",
                    err, attempt_no
                );
                attempt_no += 1;

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            _ => {
                return Err(err);
            }
        }
    }
}

pub async fn resize_page_blob<TMyPageBlob: MyPageBlob>(
    page_blob: &mut TMyPageBlob,
    pages_amount: usize,
) -> Result<(), AzureStorageError> {
    let mut attempt_no = 1;

    loop {
        let result = page_blob.resize(pages_amount).await;

        if result.is_ok() {
            return Ok(());
        }

        let err = result.err().unwrap();

        match &err {
            AzureStorageError::ContainerNotFound => {
                crate::page_blob_utils::create_container_with_retires(page_blob).await?;
            }
            AzureStorageError::HyperError { err: _ } => {
                println!(
                    "Can not execute resize_page_blob because of  {:?}. Attempt {} Retrying",
                    err, attempt_no
                );
                attempt_no += 1;

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            _ => {
                return Err(err);
            }
        }
    }
}

pub async fn read_pages<TMyPageBlob: MyPageBlob>(
    page_blob: &mut TMyPageBlob,
    start_page: usize,
    pages_amount: usize,
) -> Result<Vec<u8>, AzureStorageError> {
    let mut attempt_no = 1;

    loop {
        let result = page_blob.get(start_page, pages_amount).await;

        if let Ok(result) = result {
            return Ok(result);
        }

        let err = result.err().unwrap();

        match &err {
            AzureStorageError::ContainerNotFound => {
                crate::page_blob_utils::create_container_with_retires(page_blob).await?;
            }
            AzureStorageError::HyperError { err: _ } => {
                println!(
                    "Can not execute read_pages because of  {:?}. Attempt {} Retrying",
                    err, attempt_no
                );
                attempt_no += 1;

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            _ => {
                return Err(err);
            }
        }
    }
}

pub async fn write_pages<TMyPageBlob: MyPageBlob>(
    page_blob: &mut TMyPageBlob,
    start_page: usize,
    max_pages_to_write: usize,
    payload: Vec<u8>,
) -> Result<(), AzureStorageError> {
    let mut attempt_no = 1;

    loop {
        let payload_to_write = payload.to_vec();
        let result = page_blob
            .save_pages(start_page, max_pages_to_write, payload_to_write)
            .await;

        if result.is_ok() {
            return Ok(());
        }

        let err = result.err().unwrap();

        match &err {
            AzureStorageError::ContainerNotFound => {
                crate::page_blob_utils::create_container_with_retires(page_blob).await?;
            }
            AzureStorageError::HyperError { err: _ } => {
                println!(
                    "Can not execute write_pages because of  {:?}. Attempt {} Retrying",
                    err, attempt_no
                );
                attempt_no += 1;

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            _ => {
                return Err(err);
            }
        }
    }
}
