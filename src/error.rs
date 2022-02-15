use my_azure_storage_sdk::AzureStorageError;
#[derive(Debug)]
pub enum PageBlobAppendError {
    NotInitialized,
    Corrupted(String),
    AzureStorageError(AzureStorageError),
    Forbidden(String),
}

impl PageBlobAppendError {
    pub fn is_corrupted(&self) -> bool {
        if let Self::Corrupted(_) = self {
            return true;
        }
        return false;
    }
}

impl From<AzureStorageError> for PageBlobAppendError {
    fn from(err: AzureStorageError) -> Self {
        Self::AzureStorageError(err)
    }
}
