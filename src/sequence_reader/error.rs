use my_azure_storage_sdk::AzureStorageError;

#[derive(Debug)]
pub enum PageBlobSequenceReaderError {
    AzureStorageError(AzureStorageError),
    NoSuchAmountToRead,
}

impl From<AzureStorageError> for PageBlobSequenceReaderError {
    fn from(src: AzureStorageError) -> Self {
        Self::AzureStorageError(src)
    }
}
