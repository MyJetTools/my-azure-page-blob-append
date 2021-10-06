use my_azure_page_blob::MyPageBlob;

use super::{StateDataNotInitialized, StateDataReading, StateDataWriting};

pub enum PageBlobAppendCacheState<TMyPageBlob: MyPageBlob> {
    NotInitialized(StateDataNotInitialized<TMyPageBlob>),
    Reading(StateDataReading<TMyPageBlob>),
    Corrupted(TMyPageBlob),
    Writing(StateDataWriting<TMyPageBlob>),
}

impl<TMyPageBlob: MyPageBlob> PageBlobAppendCacheState<TMyPageBlob> {
    pub fn to_corrupted(self) -> Self {
        match self {
            PageBlobAppendCacheState::NotInitialized(state) => {
                PageBlobAppendCacheState::Corrupted(state.page_blob)
            }
            PageBlobAppendCacheState::Reading(state) => {
                PageBlobAppendCacheState::Corrupted(state.seq_reader.page_blob)
            }
            PageBlobAppendCacheState::Corrupted(blob) => PageBlobAppendCacheState::Corrupted(blob),
            PageBlobAppendCacheState::Writing(state) => {
                PageBlobAppendCacheState::Corrupted(state.seq_writer.page_blob)
            }
        }
    }
}

pub enum ChangeState {
    ToInitialization,
    ToInitialized,
    ToCorrupted,
}
