use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

use crate::{
    error::CorruptedErrorInfo, read_write::PageBlobSequenceReader,
    settings::AppendPageBlobSettings, PageBlobAppendError,
};

use super::{state::ChangeState, StateDataNotInitialized};

pub enum GetNextPayloadResult {
    NextPayload(Vec<u8>),
    ChangeState(ChangeState),
}

pub struct StateDataReading<TMyPageBlob: MyPageBlob> {
    pub seq_reader: PageBlobSequenceReader<TMyPageBlob>,
    pub pages_have_read: usize,
    pub settings: AppendPageBlobSettings,
    pub blob_size_in_pages: usize,
}

impl<TMyPageBlob: MyPageBlob> StateDataReading<TMyPageBlob> {
    pub fn from_not_initialized(
        not_initialized: StateDataNotInitialized<TMyPageBlob>,
        settings: AppendPageBlobSettings,
    ) -> Self {
        Self {
            seq_reader: PageBlobSequenceReader::new(
                not_initialized.page_blob,
                settings.cache_capacity_in_pages,
            ),
            pages_have_read: 0,

            settings,
            blob_size_in_pages: not_initialized.blob_size_in_pages,
        }
    }

    pub fn get_blob_position(&self) -> usize {
        self.seq_reader.get_blob_position()
    }

    async fn get_message_size(&mut self) -> Result<i32, PageBlobAppendError> {
        let mut buf = [0u8; 4];

        let read = self.seq_reader.read(&mut buf).await?;

        if read {
            Ok(i32::from_le_bytes(buf))
        } else {
            return Err(PageBlobAppendError::Corrupted(CorruptedErrorInfo {
                broken_pos: 0,
                last_page: None,
                msg: format!(
                    "Can not read next payload_size. Blob is corrupted. Pos:{}",
                    self.seq_reader.get_blob_position()
                ),
            }));
        }
    }

    async fn get_payload(&mut self, msg_size: i32) -> Result<Vec<u8>, PageBlobAppendError> {
        let msg_size = msg_size as usize;
        let mut buf: Vec<u8> = vec![0; msg_size];

        let read_result = self.seq_reader.read(&mut buf).await?;

        if read_result {
            Ok(buf)
        } else {
            return Err(PageBlobAppendError::Corrupted(CorruptedErrorInfo {
                last_page: None,
                broken_pos: 0,
                msg: format!(
                    "Not enought data to read payload. Blob is corrupted. Pos:{}",
                    self.seq_reader.get_blob_position()
                ),
            }));
        }
    }

    pub async fn get_next_payload(&mut self) -> Result<GetNextPayloadResult, PageBlobAppendError> {
        let (start_pos, last_page) = self
            .seq_reader
            .read_cache
            .get_last_page_remaining_content(0);

        let payload_size = self.get_message_size().await;

        if let Err(err) = payload_size {
            if let PageBlobAppendError::Corrupted(mut info) = err {
                info.broken_pos = start_pos;
                info.last_page = last_page;

                return Err(PageBlobAppendError::Corrupted(info));
            } else {
                return Err(err);
            }
        }

        let payload_size = payload_size.unwrap();

        if payload_size > self.settings.max_payload_size_protection {
            return Err(PageBlobAppendError::Corrupted(CorruptedErrorInfo {
                broken_pos: start_pos,
                last_page: last_page,
                msg: format!(
                    "Payload size {} is too huge. Maximum allowed amount is {}.",
                    payload_size, self.settings.max_payload_size_protection,
                ),
            }));
        }

        if payload_size == 0 {
            return Ok(GetNextPayloadResult::ChangeState(ChangeState::ToWriteMode));
        }

        let payload = self.get_payload(payload_size).await;

        if let Err(err) = payload {
            if let PageBlobAppendError::Corrupted(mut info) = err {
                info.broken_pos = start_pos;
                info.last_page = last_page;

                return Err(PageBlobAppendError::Corrupted(info));
            } else {
                return Err(err);
            }
        }

        return Ok(GetNextPayloadResult::NextPayload(payload.unwrap()));
    }

    pub async fn init_blob(
        &mut self,
        backup_blob: Option<&mut TMyPageBlob>,
    ) -> Result<ChangeState, AzureStorageError> {
        if let Some(backup_blob) = backup_blob {
            super::utils::copy_blob(
                &mut self.seq_reader.page_blob,
                backup_blob,
                self.settings.max_pages_to_write_single_round_trip,
            )
            .await?;
        }

        crate::with_retries::resize_page_blob(&mut self.seq_reader.page_blob, 0).await?;

        Ok(ChangeState::ToWriteMode)
    }
}
