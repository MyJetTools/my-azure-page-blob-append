use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::PageBlobSequenceReaderWithCache;

use super::{Cache, PackageBuilder};

pub struct PageBlobSequenceWriter<TPageBlob: MyPageBlob> {
    pub page_blob: TPageBlob,
    pub write_position: usize,
    pub cache: Cache,
    pub current_page: usize,
}

impl<TPageBlob: MyPageBlob> PageBlobSequenceWriter<TPageBlob> {
    pub fn new(src: PageBlobSequenceReaderWithCache<TPageBlob>) -> Self {
        let mut buffer: Vec<u8> = Vec::with_capacity(src.cache.page_size * 2);
        buffer.extend_from_slice(&src.cache.last_pages);

        let position_in_blob =
            src.current_page * src.cache.page_size + buffer.len() % src.cache.page_size;

        Self {
            page_blob: src.page_blob,
            write_position: src.position,
            cache: Cache::new(src.cache.page_size, buffer, position_in_blob),
            current_page: src.current_page,
        }
    }

    fn get_position_to_write(&self) -> usize {
        //self.cache.
        0
    }

    fn divRoundClosest(n: usize, d: usize) -> usize {
        let first = n/d;
        let last = (n % d != 0) as usize;
        
        first + last
    }

    pub async fn append(&mut self, package: &mut PackageBuilder) -> Result<(), AzureStorageError> {
        package.finalize();

        let buffer = &package.buffer;
        let position_to_write = self.get_position_to_write();
        let max_pages_to_write_single_round_trip = 2;
        let resize_pages_ration = 2;
        let current_page = self.current_page;
        let last_position = self.cache.position_in_last_pages;
        let buf_length = buffer.len();
        // understand what to do with 4 last bytes
        let mut payload: Vec<u8>;
        let from: usize;
        let to: usize;

        // self.cache.blob_is_increased(&buffer);

        // cache is not empty
        let cache_length = self.cache.data.len();
        let mut page_amount = 0;
        let mut next_page = self.current_page;
        if cache_length != 0 {
            page_amount = cache_length / self.cache.page_size;
            // previous last 4 bytes on the same page
            from = if last_position >= 4 {
                next_page -= 1;
                // send last page
                self.cache.data.len() - (last_position + self.cache.page_size * page_amount)
            } else {
                next_page -= 2;
                // send all pages
                0
            };
            to = self.cache.data.len() - 4;
            payload = self.cache.data[from..to].to_vec();
            payload.extend(buffer);
        } else {
            //cache is empty - this is first write operation
            payload = buffer.to_vec();
        }

        let payload_size = payload.len();
        let mut copy_payload: Vec<u8> = Vec::with_capacity(payload_size);
        copy_payload.extend_from_slice(&payload[..]);

        // self.page_blob.(start_page_no, max_pages_to_write, payload);
        self.page_blob
            .auto_ressize_and_save_pages(
                current_page,
                max_pages_to_write_single_round_trip,
                payload,
                resize_pages_ration,
            )
            .await?;

        // update cache
        //let previous_page = self.current_page;
        let pages_change = PageBlobSequenceWriter::<TPageBlob>::divRoundClosest(payload_size, self.cache.page_size);
        self.current_page = next_page + pages_change;
        //let pages_added = self.current_page - previous_page;

        self.cache
            .blob_is_increased(&copy_payload[..], buf_length - 4);

        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::{Cache, PackageBuilder};
    use crate::PageBlobSequenceReaderWithCache;
    use crate::PageBlobSequenceWriter;
    use my_azure_page_blob::{MyPageBlob, MyPageBlobMock};
    use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

    #[tokio::test]
    async fn basic_flow() {
        let page_size = my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;
        let first_package = [1u8; 513];

        let mut my_page_blob = MyPageBlobMock::new();
        my_page_blob.create_container_if_not_exist().await.unwrap();
        my_page_blob.create_if_not_exists(2).await.unwrap();
        let mut size: usize = 0;

        {
            let mut builder = PackageBuilder::new();
            builder.add_payload(&first_package[..]);
            builder.finalize();
            size = builder.buffer.len();
            my_page_blob
                .auto_ressize_and_save_pages(0, 2, builder.buffer, 2)
                .await
                .unwrap();
        }

        let mut reader = PageBlobSequenceReaderWithCache::new(my_page_blob, 5);

        {
            let mut out_buffer = vec![0; size];
            reader.read(&mut out_buffer).await.unwrap();
        }

        let mut writer = PageBlobSequenceWriter::new(reader);

        {
            let second_package = [2u8; 513];
            let mut builder = PackageBuilder::new();
            builder.add_payload(&second_package[..]);
            writer.append(&mut builder).await.unwrap();
        }

        {
            let pages = writer.page_blob.pages;

            for page in pages {
                print!("{:?}", page);
            }
        }
    }
}
