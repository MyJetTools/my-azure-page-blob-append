use my_azure_page_blob::*;

use crate::{
    settings::AppendPageBlobSettings,
    states::{GetNextPayloadResult, StateDataNotInitialized, StateDataReading, StateDataWriting},
    ChangeState, PageBlobAppendCacheState, PageBlobAppendError,
};

pub struct PageBlobAppend<TMyPageBlob: MyPageBlob> {
    state: Option<PageBlobAppendCacheState<TMyPageBlob>>,
    settings: AppendPageBlobSettings,
}

impl<TMyPageBlob: MyPageBlob> PageBlobAppend<TMyPageBlob> {
    pub fn new(page_blob: TMyPageBlob, settings: AppendPageBlobSettings) -> Self {
        Self {
            state: Some(PageBlobAppendCacheState::NotInitialized(
                StateDataNotInitialized::new(page_blob),
            )),
            settings,
        }
    }

    pub fn get_page_blob(&mut self) -> &mut TMyPageBlob {
        match self.state.as_mut().unwrap() {
            PageBlobAppendCacheState::NotInitialized(state) => &mut state.page_blob,
            PageBlobAppendCacheState::Reading(state) => &mut state.seq_reader.page_blob,
            PageBlobAppendCacheState::Corrupted(state) => &mut state.page_blob,
            PageBlobAppendCacheState::Writing(state) => &mut state.seq_writer.page_blob,
        }
    }

    pub async fn append_and_write<'s>(
        &mut self,
        payloads: &Vec<Vec<u8>>,
    ) -> Result<(), PageBlobAppendError> {
        match self.state.as_mut().unwrap() {
            PageBlobAppendCacheState::NotInitialized(_) => Err(PageBlobAppendError::NotInitialized),
            PageBlobAppendCacheState::Reading(_) => Err(PageBlobAppendError::NotInitialized),
            PageBlobAppendCacheState::Corrupted(state) => {
                Err(PageBlobAppendError::Forbidden(format!(
                    "You can not write to PageBlobAppend {}/{}. It's corrupted",
                    state.page_blob.get_container_name(),
                    state.page_blob.get_blob_name()
                )))
            }
            PageBlobAppendCacheState::Writing(state) => state.append_and_write(payloads).await,
        }
    }

    pub async fn get_next_payload(&mut self) -> Result<Option<Vec<u8>>, PageBlobAppendError> {
        loop {
            match self.state.as_mut().unwrap() {
                PageBlobAppendCacheState::NotInitialized(state) => {
                    let new_state = state.init().await?;
                    if let Some(new_state) = new_state {
                        self.change_state(new_state);
                    }
                }
                PageBlobAppendCacheState::Reading(state) => {
                    let result = state.get_next_payload().await;

                    match result {
                        Ok(result) => match result {
                            GetNextPayloadResult::NextPayload(payload) => return Ok(Some(payload)),

                            GetNextPayloadResult::ChangeState(new_state) => {
                                self.change_state(new_state);
                                return Ok(None);
                            }
                        },

                        Err(err) => {
                            self.handle_error(&err);
                            return Err(err);
                        }
                    }
                }
                PageBlobAppendCacheState::Corrupted(_) => {
                    return Err(PageBlobAppendError::Forbidden(
                        "Getting next payload is forbidden in corrupted mode".to_string(),
                    ));
                }
                PageBlobAppendCacheState::Writing(_) => return Ok(None),
            }
        }
    }

    pub async fn init_blob(
        &mut self,
        backup_blob: Option<&mut TMyPageBlob>,
    ) -> Result<(), PageBlobAppendError> {
        match self.state.as_mut().unwrap() {
            PageBlobAppendCacheState::NotInitialized(state) => {
                let change_state = state.init_blob().await?;
                self.change_state(change_state);
                Ok(())
            }
            PageBlobAppendCacheState::Reading(state) => {
                let change_state = state.init_blob(backup_blob).await?;
                self.change_state(change_state);
                Ok(())
            }
            PageBlobAppendCacheState::Corrupted(state) => {
                let change_state = state.init_blob(backup_blob).await?;
                self.change_state(change_state);
                Ok(())
            }
            PageBlobAppendCacheState::Writing(state) => {
                Err(PageBlobAppendError::Forbidden(format!(
                    "Operation is forbidden. PageBlobAppend {}/{} is in the {} mode",
                    state.seq_writer.page_blob.get_container_name(),
                    state.seq_writer.page_blob.get_blob_name(),
                    "Writing"
                )))
            }
        }
    }

    pub fn get_blob_position(&self) -> usize {
        if self.state.is_none() {
            return 0;
        }
        match self.state.as_ref().unwrap() {
            PageBlobAppendCacheState::NotInitialized(_) => 0,
            PageBlobAppendCacheState::Reading(state) => state.get_blob_position(),
            PageBlobAppendCacheState::Corrupted(_) => 0,
            PageBlobAppendCacheState::Writing(state) => state.get_blob_position(),
        }
    }

    fn handle_error(&mut self, err: &PageBlobAppendError) {
        if let PageBlobAppendError::Corrupted(info) = err {
            self.change_state(ChangeState::ToCorrupted(info.clone()));
        }
    }

    fn change_state(&mut self, change_state: ChangeState) {
        let mut old_state = None;
        std::mem::swap(&mut old_state, &mut self.state);

        match change_state {
            ChangeState::ToReadMode => {
                let old_state = old_state.unwrap();

                match old_state {
                    PageBlobAppendCacheState::NotInitialized(state) => {
                        let state_data: StateDataReading<TMyPageBlob> =
                            StateDataReading::from_not_initialized(state, self.settings);
                        self.state = Some(PageBlobAppendCacheState::Reading(state_data));
                    }
                    PageBlobAppendCacheState::Reading(_) => {
                        self.state = Some(old_state);
                        panic!("We are not converting from ReadMode to ReadMode");
                    }
                    PageBlobAppendCacheState::Corrupted(_) => {
                        self.state = Some(old_state);
                        panic!("We are not converting from Corrupted to ReadMode");
                    }
                    PageBlobAppendCacheState::Writing(_) => {
                        self.state = Some(old_state);
                        panic!("We are not converting from Writing to ReadMode");
                    }
                }
            }
            ChangeState::ToWriteMode => match old_state.unwrap() {
                PageBlobAppendCacheState::NotInitialized(state) => {
                    let state_data: StateDataWriting<TMyPageBlob> =
                        StateDataWriting::from_not_initialized_state(state, &self.settings);
                    self.state = Some(PageBlobAppendCacheState::Writing(state_data));
                }
                PageBlobAppendCacheState::Reading(state) => {
                    let state_data: StateDataWriting<TMyPageBlob> =
                        StateDataWriting::from_reading_state(state, &self.settings);
                    self.state = Some(PageBlobAppendCacheState::Writing(state_data));
                }
                PageBlobAppendCacheState::Corrupted(state) => {
                    let state_data: StateDataWriting<TMyPageBlob> =
                        StateDataWriting::from_corrupted_state(state, &self.settings);
                    self.state = Some(PageBlobAppendCacheState::Writing(state_data));
                }
                PageBlobAppendCacheState::Writing(_) => {
                    panic!("Can not convert from Writing to Writing state");
                }
            },
            ChangeState::ToCorrupted(info) => {
                self.state = Some(PageBlobAppendCacheState::to_corrupted(
                    old_state.unwrap(),
                    &info,
                    self.settings,
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use my_azure_page_blob::MyPageBlobMock;

    use super::*;

    #[tokio::test]
    async fn test_corrupted_and_restored() {
        const MSG_SIZE: i32 = 512;

        let mut page_blob = MyPageBlobMock::new();
        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(0).await.unwrap();

        let mut builder: Vec<u8> = Vec::new();
        builder.extend(&MSG_SIZE.to_le_bytes());

        builder.extend(&[3u8; MSG_SIZE as usize]);
        builder.extend(&[120u8; (MSG_SIZE * 2) as usize]);

        page_blob
            .auto_ressize_and_save_pages(0, 10, builder, 1)
            .await
            .unwrap();

        let settings = AppendPageBlobSettings {
            blob_auto_resize_in_pages: 1,
            cache_capacity_in_pages: 10,
            max_pages_to_write_single_round_trip: 1000,
            max_payload_size_protection: 1024 * 1024,
        };
        let mut reader = PageBlobAppend::new(page_blob, settings);

        let payload = reader.get_next_payload().await.unwrap();

        assert_eq!(&[3u8; MSG_SIZE as usize], payload.unwrap().as_slice());

        let payload = reader.get_next_payload().await;

        let err = payload.err().unwrap();
        assert_eq!(true, err.is_corrupted());

        reader.init_blob(None).await.unwrap();

        let buff_to_write = vec![5u8, 5u8, 5u8, 5u8];
        reader.append_and_write(&vec![buff_to_write]).await.unwrap();

        let result_buffer = reader.get_page_blob().download().await.unwrap();

        assert_eq!(&[4u8, 0, 0, 0, 5, 5, 5, 5], &result_buffer[516..524]);
    }
}
