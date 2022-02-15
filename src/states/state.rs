use crate::PageCache;

use super::{StateDataNotInitialized, StateDataReading, StateDataWriting};

pub enum PageBlobAppendCacheState {
    NotInitialized(StateDataNotInitialized),
    Reading(StateDataReading),
    Corrupted(Option<PageCache>),
    Writing(StateDataWriting),
}

impl PageBlobAppendCacheState {
    pub fn as_string_name(&self) -> &str {
        match self {
            PageBlobAppendCacheState::NotInitialized(_) => "NotInitialized",
            PageBlobAppendCacheState::Reading(_) => "Reading",
            PageBlobAppendCacheState::Corrupted(_) => "Corrupted",
            PageBlobAppendCacheState::Writing(_) => "Writing",
        }
    }
}
