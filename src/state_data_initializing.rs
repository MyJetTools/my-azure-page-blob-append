use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::{
    page_blob_buffer::PageBlobBuffer, settings::Settings, ChangeState,
    PageBlobSequenceReaderWithCache, StateDataNotInitialized,
};

pub enum GetNextPayloadResult {
    NextPayload(Vec<u8>),
    ChangeState(ChangeState),
    TheEnd,
}

pub struct StateDataInitializing<TMyPageBlob: MyPageBlob> {
    pub seq_reader: PageBlobSequenceReaderWithCache<TMyPageBlob>,
    pub pages_have_read: usize,
    pub blob_position: usize,
    pub settings: Settings,
    pub buffer: PageBlobBuffer,
    pub blob_size_in_pages: usize,
}

impl<TMyPageBlob: MyPageBlob> StateDataInitializing<TMyPageBlob> {
    pub fn from_not_initialized(
        not_initialized: StateDataNotInitialized<TMyPageBlob>,
        settings: Settings,
    ) -> Self {
        Self {
            seq_reader: PageBlobSequenceReaderWithCache::new(
                not_initialized.page_blob,
                settings.cache_capacity_in_pages,
            ),
            pages_have_read: 0,
            blob_position: 0,
            settings,
            blob_size_in_pages: not_initialized.blob_size_in_pages,
            buffer: PageBlobBuffer::new(BLOB_PAGE_SIZE, settings.cache_capacity_in_pages),
        }
    }

    async fn get_message_size(&mut self) -> Result<Option<i32>, AzureStorageError> {
        let mut buf = [0u8; 4];

        let read = self.seq_reader.read(&mut buf).await?;

        if read {
            Ok(Some(i32::from_le_bytes(buf)))
        } else {
            Ok(None)
        }
    }

    async fn get_payload(&mut self, msg_size: i32) -> Result<Option<Vec<u8>>, AzureStorageError> {
        let msg_size = msg_size as usize;
        let mut buf: Vec<u8> = vec![0; msg_size];

        let read = self.seq_reader.read_message_size(&mut buf).await?;

        Ok(Some(buf))
    }

    pub async fn get_next_payload(&mut self) -> Result<GetNextPayloadResult, AzureStorageError> {
        let payload_size = self.get_message_size().await?;

        if payload_size.is_none() {
            return Ok(GetNextPayloadResult::ChangeState(ChangeState::ToCorrupted));
        }

        let payload_size = payload_size.unwrap();

        if payload_size == 0 {
            return Ok(GetNextPayloadResult::TheEnd);
        }

        let payload = self.get_payload(payload_size).await?;

        if payload.is_none() {
            return Ok(GetNextPayloadResult::ChangeState(ChangeState::ToCorrupted));
        }

        return Ok(GetNextPayloadResult::NextPayload(payload.unwrap()));
    }
}

#[cfg(test)]
mod tests {
    use my_azure_page_blob::MyPageBlobMock;

    #[test]
    fn test_positive_read_sequence() {
        let first_package = [1u8; 513];

        let my_page_blob = MyPageBlobMock::new();

        //my_page_blob.a
    }
}
