use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

use crate::{ChangeState, PageBlobAppendError};

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

    pub async fn init(&mut self) -> Result<Option<ChangeState>, PageBlobAppendError> {
        let mut attemt_no = 0;
        loop {
            attemt_no += 1;
            let result = crate::with_retries::get_available_pages_amount(&mut self.page_blob).await;

            if let Ok(blob_size_in_pages) = result {
                self.blob_size_in_pages = blob_size_in_pages;
                return Ok(Some(ChangeState::ToReadMode));
            }

            let err = result.err().unwrap();

            match &err {
                my_azure_storage_sdk::AzureStorageError::ContainerNotFound => {
                    return Err(PageBlobAppendError::BlobNotFound)
                }
                my_azure_storage_sdk::AzureStorageError::BlobNotFound => {
                    return Err(PageBlobAppendError::BlobNotFound)
                }
                my_azure_storage_sdk::AzureStorageError::HyperError { err } => {
                    println!(
                        "We have problem on HTTP Level. Attempt: {} Err: {:?}",
                        attemt_no, err
                    );
                }
                _ => return Err(PageBlobAppendError::AzureStorageError(err)),
            }
        }
    }

    pub async fn init_blob(&mut self) -> Result<ChangeState, AzureStorageError> {
        crate::with_retries::create_container_if_not_exist(&mut self.page_blob).await?;
        crate::with_retries::create_blob_if_not_exists(&mut self.page_blob, 0).await?;
        Ok(ChangeState::ToReadMode)
    }
}
