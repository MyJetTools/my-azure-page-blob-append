use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;
use my_azure_storage_sdk::AzureStorageError;

use super::read_cache::ReadCache;

pub struct PageBlobSequenceReader<TPageBlob: MyPageBlob> {
    pub page_blob: TPageBlob,
    pub current_page: usize,
    pub read_cache: ReadCache,
    pub blob_size: Option<usize>,
    pub capacity_in_pages: usize,
    pub blob_size_in_pages: usize,
}

impl<TPageBlob: MyPageBlob> PageBlobSequenceReader<TPageBlob> {
    pub fn new(page_blob: TPageBlob, capacity_in_pages: usize) -> Self {
        Self {
            page_blob,
            capacity_in_pages,
            current_page: 0,
            read_cache: ReadCache::new(BLOB_PAGE_SIZE),
            blob_size: None,
            blob_size_in_pages: 0,
        }
    }

    pub async fn get_blob_size(&mut self) -> Result<usize, AzureStorageError> {
        loop {
            return match self.blob_size {
                None => {
                    self.blob_size_in_pages =
                        crate::with_retries::get_available_pages_amount(&mut self.page_blob)
                            .await?;

                    let blob_size = self.blob_size_in_pages * BLOB_PAGE_SIZE;
                    self.blob_size = Some(blob_size);
                    Ok(blob_size)
                }
                Some(blob_size) => Ok(blob_size),
            };
        }
    }

    pub fn get_blob_position(&self) -> usize {
        self.read_cache.read_blob_position
    }

    pub async fn read(&mut self, out_buffer: &mut [u8]) -> Result<bool, AzureStorageError> {
        let blob_size = self.get_blob_size().await?;

        if self.read_cache.read_blob_position + out_buffer.len() >= blob_size {
            return Ok(false);
        }

        let mut out_position: usize = 0;

        loop {
            if self.read_cache.available_to_read_size() == 0 {
                let pages_to_download =
                    if self.current_page + self.capacity_in_pages > self.blob_size_in_pages {
                        self.blob_size_in_pages - self.current_page
                    } else {
                        self.capacity_in_pages
                    };
                let buf = self
                    .page_blob
                    .get(self.current_page, pages_to_download)
                    .await?;

                self.read_cache.upload(buf);
                self.current_page += pages_to_download;
            }

            let copied = self.read_cache.copy_to(&mut out_buffer[out_position..]);

            out_position += copied;

            if out_position == out_buffer.len() {
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use my_azure_page_blob::{MyPageBlob, MyPageBlobMock};

    use crate::read_write::PackageBuilder;

    use super::*;

    #[tokio::test]
    async fn test_init_is_empty() {
        let mut page_blob = MyPageBlobMock::new();

        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(0).await.unwrap();

        let mut reader = PageBlobSequenceReader::new(page_blob, 10);
        assert_eq!(reader.read_cache.read_blob_position, 0);

        let blob_size = reader.get_blob_size().await.unwrap();

        assert_eq!(blob_size, 0);
    }

    #[tokio::test]
    async fn test_init_we_have_some_messages() {
        let mut page_blob = MyPageBlobMock::new();
        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(0).await.unwrap();

        let mut builder = PackageBuilder::new();
        builder.add_payload(&[1u8, 1u8, 1u8]);
        builder.add_payload(&[2u8, 2u8, 2u8, 2u8]);

        page_blob
            .auto_ressize_and_save_pages(0, 10, builder.get_result(), 1)
            .await
            .unwrap();

        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(0).await.unwrap();

        let mut reader = PageBlobSequenceReader::new(page_blob, 10);

        //Reading payload
        let mut data = [0u8; 4];
        reader.read(&mut data).await.unwrap();

        assert_eq!([3u8, 0u8, 0u8, 0u8], data);

        //Reading payload
        let mut data = [0u8; 3];
        reader.read(&mut data).await.unwrap();

        assert_eq!([1u8, 1u8, 1u8], data);

        //Reading payload
        let mut data = [0u8; 4];
        reader.read(&mut data).await.unwrap();

        assert_eq!([4u8, 0u8, 0u8, 0u8], data);

        //Reading payload
        let mut data = [0u8; 4];
        reader.read(&mut data).await.unwrap();

        assert_eq!([2u8, 2u8, 2u8, 2u8], data);

        //Reading payload
        let mut data = [0u8; 4];
        reader.read(&mut data).await.unwrap();

        assert_eq!([0u8, 0u8, 0u8, 0u8], data);

        let (blob_position, last_page) = reader
            .read_cache
            .get_last_page_remaining_content(crate::read_write::utils::END_MARKER.len());

        assert_eq!(15, blob_position);

        let last_page = last_page.unwrap();
        assert_eq!(
            vec![3u8, 0u8, 0, 0, 1, 1, 1, 4, 0, 0, 0, 2, 2, 2, 2],
            last_page
        );
    }

    #[tokio::test]
    async fn test_init_we_have_some_messages_and_they_are_using_full_page() {
        let mut page_blob = MyPageBlobMock::new();
        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(0).await.unwrap();

        let mut builder = PackageBuilder::new();
        builder.add_payload(&[3u8; 508]);

        page_blob
            .auto_ressize_and_save_pages(0, 10, builder.get_result(), 1)
            .await
            .unwrap();

        let mut reader = PageBlobSequenceReader::new(page_blob, 10);

        //Reading message_size
        let mut data = [0u8; 4];
        reader.read(&mut data).await.unwrap();

        assert_eq!(508i32.to_le_bytes(), data);

        //Reading payload
        let mut data = [0u8; 508];
        reader.read(&mut data).await.unwrap();

        assert_eq!([3u8; 508], data);

        //Reading end payload
        let mut data = [0u8; 4];
        reader.read(&mut data).await.unwrap();

        assert_eq!([0u8; 4], data);

        //Asserting Last Page and position

        let (blob_position, last_page) = reader
            .read_cache
            .get_last_page_remaining_content(crate::read_write::utils::END_MARKER.len());

        assert_eq!(512, blob_position);

        assert_eq!(true, last_page.is_none());
    }

    #[tokio::test]
    async fn test_init_we_have_some_messages_and_they_are_using_two_pages_second_partially() {
        const MSG_SIZE: i32 = 512;

        let mut page_blob = MyPageBlobMock::new();
        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(0).await.unwrap();

        let mut builder = PackageBuilder::new();
        builder.add_payload(&[3u8; MSG_SIZE as usize]);

        page_blob
            .auto_ressize_and_save_pages(0, 10, builder.get_result(), 1)
            .await
            .unwrap();

        let mut reader = PageBlobSequenceReader::new(page_blob, 10);

        //Reading message_size
        let mut data = [0u8; 4];
        reader.read(&mut data).await.unwrap();

        assert_eq!(MSG_SIZE.to_le_bytes(), data);

        //Reading payload
        let mut data = [0u8; MSG_SIZE as usize];
        reader.read(&mut data).await.unwrap();

        assert_eq!([3u8; MSG_SIZE as usize], data);

        //Reading end payload
        let mut data = [0u8; 4];
        reader.read(&mut data).await.unwrap();

        assert_eq!([0u8; 4], data);

        //Asserting Last Page and position

        let (blob_position, last_page) = reader
            .read_cache
            .get_last_page_remaining_content(crate::read_write::utils::END_MARKER.len());

        assert_eq!(516, blob_position);

        let last_page = last_page.unwrap();

        assert_eq!(vec![3u8; 4], last_page);
    }
}
