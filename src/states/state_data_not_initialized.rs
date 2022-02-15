use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::{blob_operations::MyPageBlobWithCache, PageCache};

pub struct StateDataNotInitialized;

pub enum InitToReadResult {
    ToWriteMode(PageCache),
    ToReadMode,
}

impl StateDataNotInitialized {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn init_to_read<TMyPageBlob: MyPageBlob>(
        &mut self,
        my_page_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
        auto_create_if_not_exist: bool,
    ) -> Result<InitToReadResult, AzureStorageError> {
        let pages_amount = match my_page_blob.get_pages_amount().await {
            Ok(result) => result,
            Err(err) => {
                if !auto_create_if_not_exist {
                    return Err(err);
                }

                my_page_blob.create_blob_if_not_exists(0).await?;
                0
            }
        };

        //TODO - UnitTests
        if pages_amount > 0 {
            Ok(InitToReadResult::ToReadMode)
        } else {
            Ok(InitToReadResult::ToWriteMode(PageCache::new(
                vec![],
                0,
                0,
                BLOB_PAGE_SIZE,
            )))
        }
    }

    pub async fn init_blob<TMyPageBlob: MyPageBlob>(
        &self,
        my_page_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
    ) -> Result<(), AzureStorageError> {
        my_page_blob.create_blob_if_not_exists(0).await
    }
}
