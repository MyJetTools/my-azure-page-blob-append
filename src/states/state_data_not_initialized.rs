use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

use crate::ChangeState;

pub struct StateDataNotInitialized<TMyPageBlob: MyPageBlob> {
    pub page_blob: TMyPageBlob,
    pub blob_size_in_pages: usize,
}

impl<TMyPageBlob: MyPageBlob> StateDataNotInitialized<TMyPageBlob> {
    pub fn new(page_blob: TMyPageBlob) -> Self {
        Self {
            page_blob,
            blob_size_in_pages: 0,
        }
    }

    pub async fn init(&mut self) -> Result<Option<ChangeState>, AzureStorageError> {
        let mut attemt_no = 0;
        loop {
            attemt_no += 1;
            let result =
                crate::page_blob_utils::get_available_pages_amount(&mut self.page_blob).await;

            if let Ok(blob_size_in_pages) = result {
                self.blob_size_in_pages = blob_size_in_pages;
                return Ok(Some(ChangeState::ToInitialization));
            }

            let err = result.err().unwrap();

            match &err {
                my_azure_storage_sdk::AzureStorageError::ContainerNotFound => {
                    self.page_blob.create_container_if_not_exist().await?;
                }
                my_azure_storage_sdk::AzureStorageError::BlobNotFound => {
                    self.page_blob.create_if_not_exists(0).await?;
                }
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
}
