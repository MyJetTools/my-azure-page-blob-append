use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

pub async fn with_retries<TMyPageBlob: MyPageBlob>(
    my_page_blob: &TMyPageBlob,
    init_pages_amount: usize,
) -> Result<usize, AzureStorageError> {
    let mut attemt_no = 0;
    loop {
        attemt_no += 1;
        let result = my_page_blob.create_if_not_exists(init_pages_amount).await;

        if result.is_ok() {
            return result;
        }

        let err = result.err().unwrap();

        match &err {
            my_azure_storage_sdk::AzureStorageError::HyperError { err } => {
                println!(
                    "We have problem on HTTP Level. Attempt: {} Err: {:?}",
                    attemt_no, err
                );
            }
            _ => return Err(err),
        }
    }
}
