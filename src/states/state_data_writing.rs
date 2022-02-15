use my_azure_page_blob::MyPageBlob;

use crate::{blob_operations::MyPageBlobWithCache, PageBlobAppendError, PageCache};

pub struct StateDataWriting {
    pub page_cache: PageCache,
}

impl StateDataWriting {
    pub fn new(mut page_cache: PageCache) -> Self {
        page_cache.gc(1);
        Self { page_cache }
    }

    pub fn get_blob_position(&self) -> usize {
        self.page_cache.get_blob_position()
    }

    pub async fn append_and_write<'s, TMyPageBlob: MyPageBlob>(
        &mut self,
        page_blob: &mut MyPageBlobWithCache<TMyPageBlob>,
        payloads: &Vec<Vec<u8>>,
    ) -> Result<(), PageBlobAppendError> {
        let page_id = self.page_cache.get_page_id_offset();

        {
            let mut writer = self.page_cache.start_writing();

            for payload in payloads {
                writer.append_payload(payload);
            }
        }

        let payload_to_upload = self.page_cache.get_payload().to_vec();

        page_blob
            .auto_resize_and_save_pages(page_id, payload_to_upload)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use my_azure_page_blob::{MyPageBlob, MyPageBlobMock};
    use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

    use super::*;

    #[tokio::test]
    async fn test_write_cases() {
        let page_blob = MyPageBlobMock::new();

        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(0).await.unwrap();

        let mut my_page_blob = MyPageBlobWithCache::new(page_blob, 10, 1);

        let page_cache = PageCache::new(vec![], 0, 0, BLOB_PAGE_SIZE);
        let mut seq_writer = StateDataWriting::new(page_cache);

        let payloads = vec![vec![1u8, 1u8, 1u8], vec![2u8, 2u8, 2u8, 2u8]];

        seq_writer
            .append_and_write(&mut my_page_blob, &payloads)
            .await
            .unwrap();

        let data = my_page_blob.page_blob.download().await.unwrap();

        assert_eq!(&[3, 0, 0, 0, 1, 1, 1, 4, 0, 0, 0, 2, 2, 2, 2], &data[..15]);
    }
}
