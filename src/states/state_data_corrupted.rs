use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

use crate::{error::CorruptedErrorInfo, AppendPageBlobSettings, ChangeState};

use super::{StateDataNotInitialized, StateDataReading};

pub struct StateDataCorrupted<TMyPageBlob: MyPageBlob> {
    pub page_blob: TMyPageBlob,
    settings: AppendPageBlobSettings,
    pub info: CorruptedErrorInfo,
}

impl<TMyPageBlob: MyPageBlob> StateDataCorrupted<TMyPageBlob> {
    pub fn from_reading_state(
        state: StateDataReading<TMyPageBlob>,
        settings: AppendPageBlobSettings,
        info: &CorruptedErrorInfo,
    ) -> Self {
        Self {
            page_blob: state.seq_reader.page_blob,
            settings,
            info: info.clone(),
        }
    }

    pub fn from_not_initialized_state(
        state: StateDataNotInitialized<TMyPageBlob>,
        settings: AppendPageBlobSettings,
        info: &CorruptedErrorInfo,
    ) -> Self {
        Self {
            page_blob: state.page_blob,
            settings,
            info: info.clone(),
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
