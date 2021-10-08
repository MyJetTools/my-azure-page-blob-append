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
        let blob_size_in_pages =
            crate::with_retries::get_available_pages_amount(&mut self.page_blob).await?;

        self.blob_size_in_pages = blob_size_in_pages;

        if self.blob_size_in_pages == 0 {
            return Ok(Some(ChangeState::ToWriteMode));
        } else {
            return Ok(Some(ChangeState::ToReadMode));
        }
    }

    pub async fn init_blob(&mut self) -> Result<ChangeState, AzureStorageError> {
        crate::with_retries::create_container_if_not_exist(&mut self.page_blob).await?;
        crate::with_retries::create_blob_if_not_exists(&mut self.page_blob, 0).await?;
        Ok(ChangeState::ToWriteMode)
    }
}
