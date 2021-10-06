use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;

pub async fn copy_blob<TMyPageBlob: MyPageBlob>(
    src: &mut TMyPageBlob,
    dest: &mut TMyPageBlob,
    max_pages_per_write: usize,
) -> Result<(), AzureStorageError> {
    let src_pages_amount = crate::with_retries::get_available_pages_amount(src).await?;

    crate::with_retries::create_container_if_not_exist(dest).await?;
    crate::with_retries::create_blob_if_not_exists(dest, src_pages_amount).await?;
    crate::with_retries::resize_page_blob(dest, src_pages_amount).await?;

    let mut page_no: usize = 0;

    while page_no < src_pages_amount {
        let remain_pages = src_pages_amount - page_no;

        let pages_to_copy = if remain_pages > max_pages_per_write {
            max_pages_per_write
        } else {
            remain_pages
        };

        let payload = crate::with_retries::read_pages(src, page_no, pages_to_copy).await?;
        crate::with_retries::write_pages(dest, page_no, max_pages_per_write, payload).await?;

        page_no += page_no;
    }

    Ok(())
}
