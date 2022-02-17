use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::{blob_operations::MyPageBlobWithCache, PageCache};

use super::PageBlobSequenceReaderError;

const KEEP_PAGES_AFTER_GC: usize = 2;

pub async fn read_buffer<'s, TPageBlob: MyPageBlob>(
    read_cache: &mut PageCache,
    page_blob: &mut MyPageBlobWithCache<TPageBlob>,
    out_buffer: &mut [u8],
    minimal_pages_amount_to_upload: usize,
    offset: usize,
) -> Result<(), PageBlobSequenceReaderError> {
    match read_cache.try_to_get_next_slice(out_buffer.len(), offset) {
        Ok(slice) => {
            out_buffer.copy_from_slice(slice);
            return Ok(());
        }
        Err(required_size_to_upload) => {
            read_cache.gc(KEEP_PAGES_AFTER_GC);
            let uploaded = upload_to_cache(
                read_cache,
                page_blob,
                required_size_to_upload,
                minimal_pages_amount_to_upload,
            )
            .await?;

            if !uploaded {
                return Err(PageBlobSequenceReaderError::NoSuchAmountToRead);
            }
        }
    };

    match read_cache.try_to_get_next_slice(out_buffer.len(), offset) {
        Ok(slice) => {
            out_buffer.copy_from_slice(slice);
            return Ok(());
        }
        Err(_) => return Err(PageBlobSequenceReaderError::NoSuchAmountToRead),
    }
}

async fn upload_to_cache<TPageBlob: MyPageBlob>(
    read_cache: &mut PageCache,
    page_blob: &mut MyPageBlobWithCache<TPageBlob>,
    required_size_to_upload: usize,
    minimal_pages_amount_to_upload: usize,
) -> Result<bool, AzureStorageError> {
    let mut required_pages_to_load =
        crate::page_blob_utils::get_pages_amount_by_size(required_size_to_upload, BLOB_PAGE_SIZE);

    if required_pages_to_load < minimal_pages_amount_to_upload {
        required_pages_to_load = minimal_pages_amount_to_upload
    }

    let remain_pages_to_load_from_blob = page_blob.get_pages_amount().await?;

    let pages_to_load = if remain_pages_to_load_from_blob < required_pages_to_load {
        remain_pages_to_load_from_blob
    } else {
        required_pages_to_load
    };

    if pages_to_load == 0 {
        return Ok(false);
    }

    let page_no = read_cache.get_next_page_after_cache();

    let payload = page_blob.read_pages(page_no, pages_to_load).await?;

    read_cache.append_payload_from_blob(payload.as_slice());

    return Ok(true);
}

#[cfg(test)]
mod tests {

    use my_azure_page_blob::{MyPageBlob, MyPageBlobMock};

    use super::*;

    fn generate_test_array(size: usize) -> Vec<u8> {
        let mut result = Vec::new();
        for i in 0..size {
            result.push(i as u8);
        }

        result
    }

    #[tokio::test]
    async fn test_if_we_read_from_empty_blob() {
        let page_blob = MyPageBlobMock::new();

        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(0).await.unwrap();

        let mut my_page_blob_with_cache = MyPageBlobWithCache::new(page_blob, 16, 2);

        let mut out_buffer = vec![0u8, 0u8, 0u8, 0u8];

        let mut read_cache = PageCache::new(BLOB_PAGE_SIZE);

        let result = super::read_buffer(
            &mut read_cache,
            &mut my_page_blob_with_cache,
            &mut out_buffer,
            10,
            0,
        )
        .await;

        if let Err(err) = result {
            if let PageBlobSequenceReaderError::NoSuchAmountToRead = err {
            } else {
                panic!("Should not be here")
            }
        } else {
            panic!("Should not be here")
        }
    }

    #[tokio::test]
    async fn test_with_some_data_in_blob() {
        let page_blob = MyPageBlobMock::new();

        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(1).await.unwrap();

        let payload = generate_test_array(BLOB_PAGE_SIZE);
        page_blob.save_pages(0, payload).await.unwrap();

        let mut my_page_blob_with_cache = MyPageBlobWithCache::new(page_blob, 16, 2);

        let mut read_cache = PageCache::new(BLOB_PAGE_SIZE);

        let mut result = vec![0u8, 0u8, 0u8, 0u8];

        super::read_buffer(
            &mut read_cache,
            &mut my_page_blob_with_cache,
            &mut result,
            10,
            0,
        )
        .await
        .unwrap();

        assert_eq!(vec!(0u8, 1u8, 2u8, 3u8), result);
    }
}
