use my_azure_page_blob::MyPageBlob;

use crate::{error::CorruptedErrorInfo, AppendPageBlobSettings};

use super::{StateDataCorrupted, StateDataNotInitialized, StateDataReading, StateDataWriting};

pub enum PageBlobAppendCacheState<TMyPageBlob: MyPageBlob> {
    NotInitialized(StateDataNotInitialized<TMyPageBlob>),
    Reading(StateDataReading<TMyPageBlob>),
    Corrupted(StateDataCorrupted<TMyPageBlob>),
    Writing(StateDataWriting<TMyPageBlob>),
}

impl<TMyPageBlob: MyPageBlob> PageBlobAppendCacheState<TMyPageBlob> {
    pub fn to_corrupted(self, info: &CorruptedErrorInfo, settings: AppendPageBlobSettings) -> Self {
        match self {
            PageBlobAppendCacheState::NotInitialized(state) => PageBlobAppendCacheState::Corrupted(
                StateDataCorrupted::from_not_initialized_state(state, settings, info.pos),
            ),
            PageBlobAppendCacheState::Reading(state) => PageBlobAppendCacheState::Corrupted(
                StateDataCorrupted::from_reading_state(state, settings, info.pos),
            ),
            _ => {
                panic!(
                    "PageBlobAppend can not be converted to corrupted state from the state {}",
                    self.as_string_name()
                )
            }
        }
    }

    pub fn as_string_name(&self) -> &str {
        match self {
            PageBlobAppendCacheState::NotInitialized(_) => "NotInitialized",
            PageBlobAppendCacheState::Reading(_) => "Reading",
            PageBlobAppendCacheState::Corrupted(_) => "Corrupted",
            PageBlobAppendCacheState::Writing(_) => "Writing",
        }
    }
}

pub enum ChangeState {
    ToReadMode,
    ToWriteMode,
    ToCorrupted(CorruptedErrorInfo),
}
