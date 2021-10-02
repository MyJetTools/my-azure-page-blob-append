use my_azure_page_blob::MyPageBlob;

use crate::{ChangeState, PageBlobAppendCacheError};

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

    pub async fn init(&mut self) -> Result<Option<ChangeState>, PageBlobAppendCacheError> {
        self.blob_size_in_pages =
            super::page_blob_utils::get_available_pages_amount(&mut self.page_blob).await?;

        return Ok(Some(ChangeState::ToInitialization));
    }
}
