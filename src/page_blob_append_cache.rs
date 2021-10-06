use my_azure_page_blob::*;

use crate::{
    settings::AppendPageBlobSettings,
    states::{GetNextPayloadResult, StateDataNotInitialized, StateDataReading, StateDataWriting},
    ChangeState, PageBlobAppendCacheError, PageBlobAppendCacheState,
};

pub struct PageBlobAppendCache<TMyPageBlob: MyPageBlob> {
    state: Option<PageBlobAppendCacheState<TMyPageBlob>>,
    settings: AppendPageBlobSettings,
}

impl<TMyPageBlob: MyPageBlob> PageBlobAppendCache<TMyPageBlob> {
    pub fn new(page_blob: TMyPageBlob, settings: AppendPageBlobSettings) -> Self {
        Self {
            state: Some(PageBlobAppendCacheState::NotInitialized(
                StateDataNotInitialized::new(page_blob),
            )),
            settings,
        }
    }

    pub async fn append_and_write<'s>(
        &mut self,
        payloads: &Vec<Vec<u8>>,
    ) -> Result<(), PageBlobAppendCacheError> {
        match self.state.as_mut().unwrap() {
            PageBlobAppendCacheState::NotInitialized(_) => {
                Err(PageBlobAppendCacheError::NotInitialized)
            }
            PageBlobAppendCacheState::Reading(_) => Err(PageBlobAppendCacheError::NotInitialized),
            PageBlobAppendCacheState::Corrupted(_) => Err(PageBlobAppendCacheError::Corrupted),
            PageBlobAppendCacheState::Writing(state) => state.append_and_write(payloads).await,
        }
    }

    pub async fn get_next_payload(&mut self) -> Result<Option<Vec<u8>>, PageBlobAppendCacheError> {
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
                    return Err(PageBlobAppendCacheError::Corrupted);
                }
                PageBlobAppendCacheState::Writing(_) => return Ok(None),
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
            ChangeState::ToInitialization => {
                if let PageBlobAppendCacheState::NotInitialized(state) = old_state.unwrap() {
                    let state_data: StateDataReading<TMyPageBlob> =
                        StateDataReading::from_not_initialized(state, self.settings);
                    self.state = Some(PageBlobAppendCacheState::Reading(state_data));
                }
            }
            ChangeState::ToInitialized => {
                if let PageBlobAppendCacheState::Reading(state) = old_state.unwrap() {
                    let state_data: StateDataWriting<TMyPageBlob> =
                        StateDataWriting::from_initializing(state, &self.settings);
                    self.state = Some(PageBlobAppendCacheState::Writing(state_data));
                }
            }
            ChangeState::ToCorrupted => {
                self.state = Some(PageBlobAppendCacheState::to_corrupted(old_state.unwrap()));
            }
        }
    }
}
