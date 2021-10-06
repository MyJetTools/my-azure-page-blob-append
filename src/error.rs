use my_azure_storage_sdk::AzureStorageError;

#[derive(Debug)]
pub enum PageBlobAppendError {
    NotInitialized,
    Corrupted(String),
    AzureStorageError(AzureStorageError),
    BlobNotFound,
    Forbidden(String),
}

impl From<AzureStorageError> for PageBlobAppendError {
    fn from(err: AzureStorageError) -> Self {
        Self::AzureStorageError(err)
    }
}
