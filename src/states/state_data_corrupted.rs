use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

use crate::{AppendPageBlobSettings, ChangeState};

use super::{StateDataNotInitialized, StateDataReading};

pub struct StateDataCorrupted<TMyPageBlob: MyPageBlob> {
    pub page_blob: TMyPageBlob,
    settings: AppendPageBlobSettings,
    pub start_pos: usize,
    pub last_page: Option<Vec<u8>>,
}

impl<TMyPageBlob: MyPageBlob> StateDataCorrupted<TMyPageBlob> {
    pub fn from_reading_state(
        mut state: StateDataReading<TMyPageBlob>,
        settings: AppendPageBlobSettings,
    ) -> Self {
        let (write_position, last_page) = state.seq_reader.read_cache.get_last_page();
        Self {
            page_blob: state.seq_reader.page_blob,
            settings,
            start_pos: write_position,
            last_page,
        }
    }

    pub fn from_not_initialized_state(
        state: StateDataNotInitialized<TMyPageBlob>,
        settings: AppendPageBlobSettings,
        start_pos: usize,
    ) -> Self {
        Self {
            page_blob: state.page_blob,
            settings,
            start_pos,
            last_page: None,
        }
    }

    pub async fn init_blob(
        &mut self,
        backup_blob: Option<&mut TMyPageBlob>,
    ) -> Result<ChangeState, AzureStorageError> {
        if let Some(backup_blob) = backup_blob {
            super::utils::copy_blob(
                &mut self.page_blob,
                backup_blob,
                self.settings.max_pages_to_write_single_round_trip,
            )
            .await?;
        }

        Ok(ChangeState::ToWriteMode)
    }
}
