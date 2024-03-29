use my_azure_storage_sdk::AzureStorageError;
#[derive(Debug, Clone)]
pub struct CorruptedErrorInfo {
    pub broken_pos: usize,
    pub msg: String,
    pub last_page: Option<Vec<u8>>,
}

#[derive(Debug)]
pub enum PageBlobAppendError {
    NotInitialized,
    Corrupted(CorruptedErrorInfo),
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
