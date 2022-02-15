use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

pub async fn copy_blob<TMyPageBlob: MyPageBlob>(
    src: &TMyPageBlob,
    dest: &TMyPageBlob,
    max_pages_per_write: usize,
) -> Result<(), AzureStorageError> {
    let src_pages_amount = super::get_available_pages_amount::with_retries(src).await?;

    super::create_container_if_not_exist::with_retries(dest).await?;
    super::create_blob_if_not_exists::with_retries(dest, src_pages_amount).await?;
    super::resize_page_blob::with_retries(dest, src_pages_amount).await?;

    let mut page_no: usize = 0;

    while page_no < src_pages_amount {
        let remain_pages = src_pages_amount - page_no;

        let pages_to_copy = if remain_pages > max_pages_per_write {
            max_pages_per_write
        } else {
            remain_pages
        };

        let payload = super::read_pages::with_retries(src, page_no, pages_to_copy).await?;
        super::write_pages::with_retries(dest, page_no, max_pages_per_write, payload).await?;

        page_no += pages_to_copy;
    }

    Ok(())
}
