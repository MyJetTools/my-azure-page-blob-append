use crate::page_blob_buffer::PageBlobBuffer;
use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;
use my_azure_storage_sdk::AzureStorageError;

pub struct PageBlobSequenceReaderWithCache<TPageBlob: MyPageBlob> {
    pub page_blob: TPageBlob,
    pub current_page: usize,
    pub cache: PageBlobBuffer,
    pub blob_size: Option<usize>,
    pub capacity_in_pages: usize,
    pub position: usize,
    pub blob_size_in_pages: usize,
}

impl<TPageBlob: MyPageBlob> PageBlobSequenceReaderWithCache<TPageBlob> {
    pub fn new(page_blob: TPageBlob, capacity_in_pages: usize) -> Self {
        Self {
            page_blob,
            capacity_in_pages,
            current_page: 0,
            position: 0,
            cache: PageBlobBuffer::new(BLOB_PAGE_SIZE, capacity_in_pages),
            blob_size: None,
            blob_size_in_pages: 0,
        }
    }

    pub async fn get_blob_size(&mut self) -> Result<usize, AzureStorageError> {
        loop {
            return match self.blob_size {
                None => {
                    self.blob_size_in_pages =
                        crate::page_blob_utils::get_available_pages_amount(&mut self.page_blob)
                            .await?;

                    let blob_size = self.blob_size_in_pages * BLOB_PAGE_SIZE;
                    self.blob_size = Some(blob_size);
                    Ok(blob_size)
                }
                Some(blob_size) => Ok(blob_size),
            };
        }
    }

    pub async fn read(&mut self, out_buffer: &mut [u8]) -> Result<bool, AzureStorageError> {
        let blob_size = self.get_blob_size().await?;

        if self.position + out_buffer.len() >= blob_size {
            return Ok(false);
        }

        let mut out_position: usize = 0;

        loop {
            if self.cache.available_to_read_size() == 0 {
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

                self.cache.upload(buf.as_slice());
                self.current_page += pages_to_download;
            }

            let copied = self.cache.copy_to(&mut out_buffer[out_position..]);

            out_position += copied;

            if out_position == out_buffer.len() {
                return Ok(true);
            }
        }
    }
}
