use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::settings::AppendPageBlobSettings;

use super::{PackageBuilder, PageBlobSequenceReader, WriteCache};

pub struct PageBlobSequenceWriter<TPageBlob: MyPageBlob> {
    pub page_blob: TPageBlob,
    pub write_cache: WriteCache,
    pub max_pages_to_write: usize,
    blob_autoressize_in_pages: usize,
}

impl<TPageBlob: MyPageBlob> PageBlobSequenceWriter<TPageBlob> {
    pub fn new(
        mut reader: PageBlobSequenceReader<TPageBlob>,
        settings: &AppendPageBlobSettings,
    ) -> Self {
        let (write_position, last_page) = reader.read_cache.get_last_page();
        Self {
            page_blob: reader.page_blob,
            max_pages_to_write: 4000,
            blob_autoressize_in_pages: settings.blob_auto_resize_in_pages,
            write_cache: WriteCache::new(BLOB_PAGE_SIZE, last_page, write_position),
        }
    }

    pub async fn append(&mut self, package: PackageBuilder) -> Result<(), AzureStorageError> {
        let payload_to_write = package.get_result();

        self.write_cache.start_increasing_blob(&payload_to_write);

        let payload_to_write = self
            .write_cache
            .concat_with_current_cache(&payload_to_write);

        let page_no = super::utils::get_page_no_from_page_blob_position(
            self.write_cache.write_position,
            BLOB_PAGE_SIZE,
        );

        self.page_blob
            .auto_ressize_and_save_pages(
                page_no,
                self.max_pages_to_write,
                payload_to_write,
                self.blob_autoressize_in_pages,
            )
            .await?;

        self.write_cache.written();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use my_azure_page_blob::MyPageBlobMock;

    use super::*;

    #[tokio::test]
    async fn test_write_cases() {
        let mut page_blob = MyPageBlobMock::new();

        page_blob.create_container_if_not_exist().await.unwrap();

        page_blob.create_if_not_exists(0).await.unwrap();

        let reader = PageBlobSequenceReader::new(page_blob, 10);

        let settings = AppendPageBlobSettings {
            blob_auto_resize_in_pages: 2,
            cache_capacity_in_pages: 1,
            max_pages_to_write_single_round_trip: 4000,
            max_payload_size_protection: 1,
        };

        let mut seq_writer = PageBlobSequenceWriter::new(reader, &settings);

        let mut package_builder = PackageBuilder::new();

        package_builder.add_payload(&[1u8, 1u8, 1u8]);
        package_builder.add_payload(&[2u8, 2u8, 2u8, 2u8]);

        seq_writer.append(package_builder).await.unwrap();

        let data = seq_writer.page_blob.download().await.unwrap();

        assert_eq!(&[3, 0, 0, 0, 1, 1, 1, 4, 0, 0, 0, 2, 2, 2, 2], &data[..15]);
    }
}
