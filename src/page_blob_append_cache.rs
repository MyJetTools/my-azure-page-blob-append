use my_azure_page_blob::*;

use crate::{
    settings::Settings, state_data_initializing::GetNextPayloadResult, ChangeState,
    PageBlobAppendCacheError, PageBlobAppendCacheState,
    StateDataInitializing, StateDataNotInitialized,
};

pub struct PageBlobAppendCache<TMyPageBlob: MyPageBlob> {
    state: Option<PageBlobAppendCacheState<TMyPageBlob>>,
    settings: Settings,
}

impl<TMyPageBlob: MyPageBlob> PageBlobAppendCache<TMyPageBlob> {
    pub fn new(page_blob: TMyPageBlob, settings: Settings) -> Self {
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
            PageBlobAppendCacheState::Initializing(_) => {
                Err(PageBlobAppendCacheError::NotInitialized)
            }
            PageBlobAppendCacheState::Corrupted(_) => Err(PageBlobAppendCacheError::Corrupted),
            PageBlobAppendCacheState::Initialized(state) => {
                state
                    .append_and_write(payloads, self.settings.max_pages_to_write_single_round_trip)
                    .await
            }
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
                PageBlobAppendCacheState::Initializing(state) => {
                    let result = state.get_next_payload().await?;

                    match result {
                        GetNextPayloadResult::NextPayload(payload) => return Ok(Some(payload)),

                        GetNextPayloadResult::ChangeState(new_state) => {
                            self.change_state(new_state);
                            return Ok(None);
                        }
                        GetNextPayloadResult::TheEnd => todo!("THE END ARM!"),
                    }
                }
                PageBlobAppendCacheState::Corrupted(_) => {
                    return Err(PageBlobAppendCacheError::Corrupted);
                }
                PageBlobAppendCacheState::Initialized(_) => return Ok(None),
            }
        }
    }

    fn change_state(&mut self, change_state: ChangeState) {
        let mut old_state = None;
        std::mem::swap(&mut old_state, &mut self.state);

        match change_state {
            ChangeState::ToInitialization => {
                if let PageBlobAppendCacheState::NotInitialized(state) = old_state.unwrap() {
                    let state_data: StateDataInitializing<TMyPageBlob> =
                        StateDataInitializing::from_not_initialized(state, self.settings);
                    self.state = Some(PageBlobAppendCacheState::Initializing(state_data));
                }
            }
            ChangeState::ToInitialized => {
                if let PageBlobAppendCacheState::Initializing(state) = old_state.unwrap() {
                    todo!("Fix commented");
                    /* let state_data: StateDataInitialized<TMyPageBlob> =
                        StateDataInitialized::from_initializing(state);
                    self.state = Some(PageBlobAppendCacheState::Initialized(state_data)); */
                }
            }
            ChangeState::ToCorrupted => {
                self.state = Some(PageBlobAppendCacheState::to_corrupted(old_state.unwrap()));
            }
            _ => {
                std::mem::swap(&mut old_state, &mut self.state);
            }
        }
    }

    fn get_page_blob_mut(&mut self) -> &mut TMyPageBlob {
        match self.state.as_mut().unwrap() {
            PageBlobAppendCacheState::NotInitialized(state) => return &mut state.page_blob,
            PageBlobAppendCacheState::Initializing(state) => {
                todo!("FIX IT");
                //return &mut state.page_blob,
            }
            PageBlobAppendCacheState::Corrupted(blob) => return blob,
            PageBlobAppendCacheState::Initialized(state) => &mut state.page_blob,
        }
    }

    async fn create_new_blob(&mut self) -> Result<(), PageBlobAppendCacheError> {
        let auto_ressize = self.settings.blob_auto_resize_in_pages;
        let page_blob = self.get_page_blob_mut();
        page_blob.create_if_not_exists(auto_ressize).await?;
        page_blob.resize(auto_ressize);
        Ok(())
    }
}