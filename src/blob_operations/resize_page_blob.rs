use std::time::Duration;

use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

pub async fn with_retries<TMyPageBlob: MyPageBlob>(
    page_blob: &TMyPageBlob,
    pages_amount: usize,
) -> Result<(), AzureStorageError> {
    let mut attempt_no = 1;

    loop {
        let result = page_blob.resize(pages_amount).await;

        if result.is_ok() {
            return result;
        }

        let err = result.err().unwrap();

        match &err {
            AzureStorageError::ContainerNotFound => {
                super::create_container_if_not_exist::with_retries(page_blob).await?;
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
