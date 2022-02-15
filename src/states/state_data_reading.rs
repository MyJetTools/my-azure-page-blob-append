use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

use crate::{
    blob_operations::MyPageBlobWithCache,
    sequence_reader::{PageBlobSequenceReader, PageBlobSequenceReaderError},
    settings::AppendPageBlobSettings,
    PageBlobAppendCacheState, PageCache,
};

const PAYLOAD_SIZE_LEN: usize = 4;
const KEEP_PAGES_AFTER_GC: usize = 2;
pub enum DataReadingErrorResult {
    AzureStorageError(AzureStorageError),
    Corrupted { msg: String, pages_cache: PageCache },
}

pub enum GetNextPayloadResult {
    NextPayload(Vec<u8>),
    GoToWriteMode(PageCache),
}

pub struct StateDataReading {
    pub seq_reader: Option<PageBlobSequenceReader>,
    pub pages_have_read: usize,
    pub settings: AppendPageBlobSettings,
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
            seq_reader: PageBlobSequenceReader::new(settings.cache_capacity_in_pages).into(),
            pages_have_read: 0,
            settings,
        }
    }

    pub fn get_pages_cache(&mut self) -> PageCache {
        let mut result = None;

        std::mem::swap(&mut self.seq_reader, &mut result);

        if result.is_none() {
            panic!("We are removing page cache for the second time");
        }

        result.unwrap().read_cache
    }

    fn get_seq_reader(&self) -> &PageBlobSequenceReader {
        match &self.seq_reader {
            Some(reader) => reader,
            None => {
                panic!("Can not get sequence reader. Object has been already disposed");
            }
        }
    }

    fn get_seq_reader_mut(&mut self) -> &mut PageBlobSequenceReader {
        match &mut self.seq_reader {
            Some(reader) => reader,
            None => {
                panic!("Can not get sequence reader. Object has been already disposed");
            }
        }
    }
    pub fn get_blob_position(&self) -> usize {
        self.get_seq_reader().read_cache.get_blob_position()
    }

    async fn download_payload<TMyPageBlob: MyPageBlob>(
        &mut self,
        page_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
        buf: &mut [u8],
    ) -> Result<(), DataReadingErrorResult> {
        let seq_reader = self.get_seq_reader_mut();

        match seq_reader.read_buffer(page_blob, buf).await {
            Ok(_) => Ok(()),
            Err(err) => match err {
                PageBlobSequenceReaderError::AzureStorageError(azure_error) => {
                    Err(DataReadingErrorResult::AzureStorageError(azure_error))
                }
                PageBlobSequenceReaderError::NoSuchAmountToRead => {
                    let  msg= format!(
                        "Can not read next payload_size. Not enough data in blob. Blob is corrupted. Pos:{}",
                        seq_reader.read_cache.get_blob_position()
                    );
                    Err(DataReadingErrorResult::Corrupted {
                        msg,
                        pages_cache: self.get_pages_cache(),
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
        self.download_payload(page_blob, &mut buf).await?;
        Ok(u32::from_le_bytes(buf))
    }

    async fn get_payload<TMyPageBlob: MyPageBlob>(
        &mut self,
        page_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
        payload_size: usize,
    ) -> Result<Vec<u8>, DataReadingErrorResult> {
        let mut buf: Vec<u8> = vec![0; payload_size];
        self.download_payload(page_blob, &mut buf).await?;
        Ok(buf)
    }

    pub async fn get_next_payload<TMyPageBlob: MyPageBlob>(
        &mut self,
        page_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
    ) -> Result<GetNextPayloadResult, DataReadingErrorResult> {
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
                pages_cache: self.get_pages_cache(),
            });
        }

        if payload_size == 0 {
            let pages_cache = self.get_pages_cache();
            return Ok(GetNextPayloadResult::GoToWriteMode(pages_cache));
        }

        self.get_seq_reader_mut()
            .read_cache
            .advance_blob_position(PAYLOAD_SIZE_LEN);

        let payload = self.get_payload(page_blob, payload_size as usize).await?;

        let seq_reader = self.get_seq_reader_mut();

        seq_reader.read_cache.advance_blob_position(payload.len());
        seq_reader.read_cache.gc(KEEP_PAGES_AFTER_GC);

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
