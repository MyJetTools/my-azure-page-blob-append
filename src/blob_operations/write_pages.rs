use std::time::Duration;

use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

pub async fn with_retries<TMyPageBlob: MyPageBlob>(
    page_blob: &TMyPageBlob,
    start_page_no: usize,
    max_pages_to_write_per_request: usize,
    payload: Vec<u8>,
) -> Result<(), AzureStorageError> {
    let mut attempt_no = 1;

    loop {
        let result = my_azure_page_blob::my_page_blob_utils::write_pages(
            page_blob,
            start_page_no,
            max_pages_to_write_per_request,
            payload.to_vec(),
        )
        .await;

        if result.is_ok() {
            return Ok(());
        }

        let err = result.err().unwrap();

        match &err {
            AzureStorageError::ContainerNotFound => {
                super::create_container_if_not_exist::with_retries(page_blob).await?;
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
