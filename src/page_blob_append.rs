use my_azure_page_blob::*;

use crate::{
    blob_operations::MyPageBlobWithCache,
    settings::AppendPageBlobSettings,
    states::{
        DataReadingErrorResult, GetNextPayloadResult, InitToReadResult, StateDataNotInitialized,
        StateDataReading, StateDataWriting,
    },
    PageBlobAppendCacheState, PageBlobAppendError, PageCache,
};

pub struct PageBlobAppend<TMyPageBlob: MyPageBlob> {
    page_blob_with_cache: MyPageBlobWithCache<TMyPageBlob>,
    state: PageBlobAppendCacheState,
    settings: AppendPageBlobSettings,
}

impl<TMyPageBlob: MyPageBlob> PageBlobAppend<TMyPageBlob> {
    pub fn new(page_blob: TMyPageBlob, settings: AppendPageBlobSettings) -> Self {
        Self {
            page_blob_with_cache: MyPageBlobWithCache::new(
                page_blob,
                settings.max_pages_to_write_single_round_trip,
                settings.blob_auto_resize_in_pages,
            ),
            state: PageBlobAppendCacheState::NotInitialized(StateDataNotInitialized::new()),
            settings,
        }
    }

    pub fn get_page_blob_mut(&mut self) -> &mut TMyPageBlob {
        return &mut self.page_blob_with_cache.page_blob;
    }

    pub fn get_page_blob(&self) -> &TMyPageBlob {
        return &self.page_blob_with_cache.page_blob;
    }

    pub async fn append_and_write<'s>(
        &mut self,
        payloads: &Vec<Vec<u8>>,
    ) -> Result<(), PageBlobAppendError> {
        if let PageBlobAppendCacheState::Writing(state) = &mut self.state {
            state
                .append_and_write(&mut self.page_blob_with_cache, payloads)
                .await?;
            return Ok(());
        }

        panic!(
            "append_and_write operation can not be performed in {} state",
            self.state.as_string_name()
        );
    }

    pub async fn force_to_write_mode(&mut self) -> Result<(), PageBlobAppendError> {
        match &mut self.state {
            PageBlobAppendCacheState::Corrupted(page_cache) => {
                let mut page_cache_to_write = None;
                std::mem::swap(page_cache, &mut page_cache_to_write);

                if page_cache_to_write.is_none() {
                    panic!("Can not get page_cache from currupted state");
                }

                self.force_switch_to_write_mode(page_cache_to_write.unwrap())
                    .await?;

                Ok(())
            }

            PageBlobAppendCacheState::Reading(state) => {
                let page_cache = state.dispose_pages_cache();
                self.force_switch_to_write_mode(page_cache).await?;
                Ok(())
            }

            _ => {
                panic!(
                    "AppendBlob can not be changed to write mode from the state {}",
                    self.state.as_string_name()
                );
            }
        }
    }

    async fn force_switch_to_write_mode(
        &mut self,
        page_cache: PageCache,
    ) -> Result<(), PageBlobAppendError> {
        let mut state_data = StateDataWriting::new(page_cache);

        state_data
            .reset_current_position_as_end_marker(&mut self.page_blob_with_cache)
            .await?;

        self.state = PageBlobAppendCacheState::Writing(state_data);

        Ok(())
    }

    pub async fn initialize_to_read_mode(
        &mut self,
        auto_create_if_not_exist: bool,
    ) -> Result<(), PageBlobAppendError> {
        let change_state = if let PageBlobAppendCacheState::NotInitialized(state) = &mut self.state
        {
            state
                .init_to_read(&mut self.page_blob_with_cache, auto_create_if_not_exist)
                .await?
        } else {
            panic!(
                "Page blob append can not be intialize to read in {} mode",
                self.state.as_string_name()
            )
        };

        match change_state {
            InitToReadResult::ToWriteMode(page_cache) => {
                self.state = PageBlobAppendCacheState::Writing(StateDataWriting::new(page_cache));
            }
            InitToReadResult::ToReadMode => {
                self.state = PageBlobAppendCacheState::Reading(StateDataReading::new(
                    &self.state,
                    self.settings,
                ));
            }
        }
        Ok(())
    }

    pub async fn get_next_payload(&mut self) -> Result<Option<Vec<u8>>, PageBlobAppendError> {
        let next_payload_result = if let PageBlobAppendCacheState::Reading(state) = &mut self.state
        {
            match state.get_next_payload(&mut self.page_blob_with_cache).await {
                Ok(result) => result,
                Err(err) => match err {
                    DataReadingErrorResult::AzureStorageError(azure_err) => {
                        return Err(PageBlobAppendError::AzureStorageError(azure_err));
                    }
                    DataReadingErrorResult::Corrupted { msg, pages_cache } => {
                        self.state = PageBlobAppendCacheState::Corrupted(Some(pages_cache));
                        return Err(PageBlobAppendError::Corrupted(msg));
                    }
                },
            }
        } else {
            panic!(
                "Getting next payload is forbidden in {} mode",
                self.state.as_string_name()
            );
        };

        match next_payload_result {
            GetNextPayloadResult::NextPayload(payload) => return Ok(Some(payload)),
            GetNextPayloadResult::GoToWriteMode(page_cache) => {
                self.state = PageBlobAppendCacheState::Writing(StateDataWriting::new(page_cache));
                return Ok(None);
            }
        }
    }

    pub fn get_blob_position(&self) -> usize {
        match &self.state {
            PageBlobAppendCacheState::NotInitialized(_) => 0,
            PageBlobAppendCacheState::Reading(state) => state.get_blob_position(),
            PageBlobAppendCacheState::Corrupted(_) => 0,
            PageBlobAppendCacheState::Writing(state) => state.get_blob_position(),
        }
    }

    pub fn is_reading_mode(&self) -> bool {
        match &self.state {
            PageBlobAppendCacheState::NotInitialized(_) => false,
            PageBlobAppendCacheState::Reading(_) => true,
            PageBlobAppendCacheState::Corrupted(_) => false,
            PageBlobAppendCacheState::Writing(_) => false,
        }
    }

    pub fn is_writing_mode(&self) -> bool {
        match &self.state {
            PageBlobAppendCacheState::NotInitialized(_) => false,
            PageBlobAppendCacheState::Reading(_) => false,
            PageBlobAppendCacheState::Corrupted(_) => false,
            PageBlobAppendCacheState::Writing(_) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use my_azure_page_blob::MyPageBlobMock;
    use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

    use crate::page_blob_utils::*;

    use super::*;

    fn made_uploadedble(buffer: &mut Vec<u8>) {
        while buffer.len() < BLOB_PAGE_SIZE {
            buffer.push(0u8);
        }
    }

    #[tokio::test]
    async fn test_corrupted_and_restored() {
        const MSG_SIZE: i32 = 512;

        //Prepare page blob
        let page_blob = MyPageBlobMock::new();
        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(0).await.unwrap();

        let mut init_payload: Vec<u8> = Vec::new();
        init_payload.extend_from_slice(&MSG_SIZE.to_le_bytes());

        init_payload.extend_from_slice(&[3u8; MSG_SIZE as usize]);
        init_payload.extend_from_slice(&[120u8; (MSG_SIZE * 2) as usize]);

        extend_buffer_to_full_pages_size(&mut init_payload, 512);

        page_blob.resize(4).await.unwrap();

        page_blob.save_pages(0, init_payload).await.unwrap();

        let settings = AppendPageBlobSettings {
            blob_auto_resize_in_pages: 1,
            cache_capacity_in_pages: 10,
            max_pages_to_write_single_round_trip: 1000,
            max_payload_size_protection: 1024 * 1024,
        };
        let mut reader = PageBlobAppend::new(page_blob, settings);

        reader.initialize_to_read_mode(false).await.unwrap();

        let payload = reader.get_next_payload().await.unwrap();

        assert_eq!(&[3u8; MSG_SIZE as usize], payload.unwrap().as_slice());

        let payload = reader.get_next_payload().await;

        let err = payload.err().unwrap();
        assert_eq!(true, err.is_corrupted());

        reader.force_to_write_mode().await.unwrap();

        let buff_to_write = vec![5u8, 5u8, 5u8, 5u8];
        reader.append_and_write(&vec![buff_to_write]).await.unwrap();

        let result_buffer = reader.get_page_blob_mut().download().await.unwrap();

        assert_eq!(&[4u8, 0, 0, 0, 5, 5, 5, 5], &result_buffer[516..524]);
    }

    #[tokio::test]
    async fn test_switch_to_write_mode() {
        let page_blob = MyPageBlobMock::new();

        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(1).await.unwrap();

        let mut payload_to_upload = vec![3u8, 0u8, 0u8, 0u8, 1u8, 2u8, 3u8, 0u8, 0u8, 0u8, 0u8];

        made_uploadedble(&mut payload_to_upload);

        page_blob.save_pages(0, payload_to_upload).await.unwrap();

        let settings = AppendPageBlobSettings {
            blob_auto_resize_in_pages: 1,
            cache_capacity_in_pages: 10,
            max_pages_to_write_single_round_trip: 1000,
            max_payload_size_protection: 1024 * 1024,
        };

        let mut reader = PageBlobAppend::new(page_blob, settings);

        reader.initialize_to_read_mode(false).await.unwrap();

        let result = reader.get_next_payload().await.unwrap();
        assert_eq!(true, result.is_some());
        assert_eq!(true, reader.is_reading_mode());

        let result = reader.get_next_payload().await.unwrap();
        assert_eq!(true, result.is_none());
        assert_eq!(true, reader.is_writing_mode());

        assert_eq!(7, reader.get_blob_position());
    }
}
