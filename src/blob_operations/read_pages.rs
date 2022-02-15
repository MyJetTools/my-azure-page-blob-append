use std::time::Duration;

use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

pub async fn with_retries<TMyPageBlob: MyPageBlob>(
    page_blob: &TMyPageBlob,
    start_page: usize,
    pages_to_read: usize,
) -> Result<Vec<u8>, AzureStorageError> {
    let mut attempt_no = 1;

    loop {
        let result = page_blob.get(start_page, pages_to_read).await;

        if let Ok(result) = result {
            return Ok(result);
        }

        let err = result.err().unwrap();

        match &err {
            AzureStorageError::ContainerNotFound => {
                super::create_container_if_not_exist::with_retries(page_blob).await?;
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
