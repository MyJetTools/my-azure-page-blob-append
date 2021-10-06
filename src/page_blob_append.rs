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
                Err(PageBlobAppendError::Corrupted(format!(
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
                    let result = state.get_next_payload().await?;

                    match result {
                        GetNextPayloadResult::NextPayload(payload) => return Ok(Some(payload)),

                        GetNextPayloadResult::ChangeState(new_state) => {
                            self.change_state(new_state);
                            return Ok(None);
                        }
                    }
                }
                PageBlobAppendCacheState::Corrupted(_) => {
                    return Err(PageBlobAppendError::Corrupted(
                        "You can not get next payload. Blob is corrupted".to_string(),
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
                Err(PageBlobAppendError::Forbidden(format!(
                    "Operation is forbidden. PageBlobAppend {}/{} append is in the {} mode",
                    state.seq_reader.page_blob.get_container_name(),
                    state.seq_reader.page_blob.get_blob_name(),
                    "Reading"
                )))
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

    fn change_state(&mut self, change_state: ChangeState) {
        let mut old_state = None;
        std::mem::swap(&mut old_state, &mut self.state);

        match change_state {
            ChangeState::ToReadMode => {
                if let PageBlobAppendCacheState::NotInitialized(state) = old_state.unwrap() {
                    let state_data: StateDataReading<TMyPageBlob> =
                        StateDataReading::from_not_initialized(state, self.settings);
                    self.state = Some(PageBlobAppendCacheState::Reading(state_data));
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
            ChangeState::ToCorrupted => {
                self.state = Some(PageBlobAppendCacheState::to_corrupted(
                    old_state.unwrap(),
                    self.settings,
                ));
            }
        }
    }
}
