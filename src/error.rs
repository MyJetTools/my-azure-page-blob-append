use my_azure_storage_sdk::AzureStorageError;

#[derive(Debug)]
pub enum PageBlobAppendError {
    NotInitialized,
    Corrupted,
    MaxSizeProtection { limit: usize, size_from_blob: usize },
    AzureStorageError(AzureStorageError),
}

impl From<AzureStorageError> for PageBlobAppendError {
    fn from(err: AzureStorageError) -> Self {
        Self::AzureStorageError(err)
    }
}
