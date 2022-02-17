use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::{
    blob_operations::MyPageBlobWithCache, sequence_reader::PageBlobSequenceReaderError,
    settings::AppendPageBlobSettings, PageBlobAppendCacheState, PageCache,
};

const PAYLOAD_SIZE_LEN: usize = 4;

pub enum DataReadingErrorResult {
    AzureStorageError(AzureStorageError),
    Corrupted { msg: String, pages_cache: PageCache },
}

pub enum GetNextPayloadResult {
    NextPayload(Vec<u8>),
    GoToWriteMode(PageCache),
}

pub struct StateDataReading {
    read_cache: Option<PageCache>,
    pub pages_have_read: usize,
    pub settings: AppendPageBlobSettings,
    advance_from_previous_payload: Option<usize>,
}

impl StateDataReading {
    pub fn new(state: &PageBlobAppendCacheState, settings: AppendPageBlobSettings) -> Self {
        match state {
            PageBlobAppendCacheState::NotInitialized(_) => Self::from_not_initialized(settings),
            _ => {
                panic!(
                    "StateDataReading can no be created from state {}",
                    state.as_string_name()
                );
            }
        }
    }

    fn from_not_initialized(settings: AppendPageBlobSettings) -> Self {
        Self {
            read_cache: PageCache::new(BLOB_PAGE_SIZE).into(),
            pages_have_read: 0,
            settings,
            advance_from_previous_payload: None,
        }
    }

    pub fn dispose_pages_cache(&mut self) -> PageCache {
        let mut result = None;

        std::mem::swap(&mut self.read_cache, &mut result);

        if result.is_none() {
            panic!("We are removing page cache for the second time");
        }

        result.unwrap()
    }

    fn get_pages_cache(&self) -> &PageCache {
        match &self.read_cache {
            Some(reader) => reader,
            None => {
                panic!("Can not get sequence reader. Object has been already disposed");
            }
        }
    }

    fn get_pages_cache_mut(&mut self) -> &mut PageCache {
        match &mut self.read_cache {
            Some(reader) => reader,
            None => {
                panic!("Can not get sequence reader. Object has been already disposed");
            }
        }
    }
    pub fn get_blob_position(&self) -> usize {
        self.get_pages_cache().get_blob_position()
    }

    async fn download_payload<TMyPageBlob: MyPageBlob>(
        &mut self,
        page_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
        buf: &mut [u8],
        offset: usize,
    ) -> Result<(), DataReadingErrorResult> {
        let minimal_pages_amount_to_upload = self.settings.cache_capacity_in_pages;

        let pages_cache = self.get_pages_cache_mut();

        match crate::sequence_reader::read_buffer(
            pages_cache,
            page_blob,
            buf,
            minimal_pages_amount_to_upload,
            offset,
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(err) => match err {
                PageBlobSequenceReaderError::AzureStorageError(azure_error) => {
                    Err(DataReadingErrorResult::AzureStorageError(azure_error))
                }
                PageBlobSequenceReaderError::NoSuchAmountToRead => {
                    let  msg= format!(
                        "Can not read next payload_size. Not enough data in blob. Blob is corrupted. Pos:{}",
                        pages_cache.get_blob_position()
                    );
                    Err(DataReadingErrorResult::Corrupted {
                        msg,
                        pages_cache: self.dispose_pages_cache(),
                    })
                }
            },
        }
    }

    async fn get_message_size<TMyPageBlob: MyPageBlob>(
        &mut self,
        page_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
    ) -> Result<u32, DataReadingErrorResult> {
        let mut buf = [0u8; PAYLOAD_SIZE_LEN];
        self.download_payload(page_blob, &mut buf, 0).await?;
        Ok(u32::from_le_bytes(buf))
    }

    async fn get_payload<TMyPageBlob: MyPageBlob>(
        &mut self,
        page_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
        payload_size: usize,
    ) -> Result<Vec<u8>, DataReadingErrorResult> {
        let mut buf: Vec<u8> = vec![0; payload_size];
        self.download_payload(page_blob, &mut buf, PAYLOAD_SIZE_LEN)
            .await?;
        Ok(buf)
    }

    pub async fn get_next_payload<TMyPageBlob: MyPageBlob>(
        &mut self,
        page_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
    ) -> Result<GetNextPayloadResult, DataReadingErrorResult> {
        if let Some(advance_from_previous_payload) = self.advance_from_previous_payload {
            self.get_pages_cache_mut()
                .advance_blob_position(advance_from_previous_payload);

            self.advance_from_previous_payload = None;
        }

        let payload_size = self.get_message_size(page_blob).await?;

        if payload_size > self.settings.max_payload_size_protection {
            let msg = format!(
                "Payload size {} is too huge. Maximum allowed size is {}. Pos {}",
                payload_size,
                self.settings.max_payload_size_protection,
                self.get_blob_position()
            );

            return Err(DataReadingErrorResult::Corrupted {
                msg,
                pages_cache: self.dispose_pages_cache(),
            });
        }

        if payload_size == 0 {
            let pages_cache = self.dispose_pages_cache();
            return Ok(GetNextPayloadResult::GoToWriteMode(pages_cache));
        }

        let payload = self.get_payload(page_blob, payload_size as usize).await?;

        self.advance_from_previous_payload = Some(payload.len() + PAYLOAD_SIZE_LEN);

        return Ok(GetNextPayloadResult::NextPayload(payload));
    }

    pub async fn init_corrupted_blob<TMyPageBlob: MyPageBlob>(
        &mut self,
        src_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
        backup_blob: Option<&TMyPageBlob>,
    ) -> Result<(), AzureStorageError> {
        if let Some(backup_blob) = backup_blob {
            crate::blob_operations::copy_blob(
                &src_blob.page_blob,
                backup_blob,
                self.settings.max_pages_to_write_single_round_trip,
            )
            .await?;
        }

        src_blob.resize_page_blob(0).await?;

        Ok(())
    }
}
