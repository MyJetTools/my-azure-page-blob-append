use my_azure_page_blob::MyPageBlob;

use crate::{StateDataInitializing, StateDataNotInitialized, StateDataWriting};

pub enum PageBlobAppendCacheState<TMyPageBlob: MyPageBlob> {
    NotInitialized(StateDataNotInitialized<TMyPageBlob>),
    Initializing(StateDataInitializing<TMyPageBlob>),
    Corrupted(TMyPageBlob),
    Initialized(StateDataWriting<TMyPageBlob>),
}

impl<TMyPageBlob: MyPageBlob> PageBlobAppendCacheState<TMyPageBlob> {
    pub fn to_corrupted(self) -> Self {
        match self {
            PageBlobAppendCacheState::NotInitialized(state) => {
                PageBlobAppendCacheState::Corrupted(state.page_blob)
            }
            PageBlobAppendCacheState::Initializing(state) => {
                PageBlobAppendCacheState::Corrupted(state.seq_reader.page_blob)
            }
            PageBlobAppendCacheState::Corrupted(blob) => PageBlobAppendCacheState::Corrupted(blob),
            PageBlobAppendCacheState::Initialized(state) => {
                PageBlobAppendCacheState::Corrupted(state.page_blob_seq_writer.page_blob)
            }
        }
    }
}

pub enum ChangeState {
    ToInitialization,
    ToInitialized,
    ToCorrupted,
}
