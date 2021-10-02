use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

use crate::PageBlobSequenceReaderWithCache;

use super::{Cache, PackageBuilder};

pub struct PageBlobSequenceWriter<TPageBlob: MyPageBlob> {
    pub page_blob: TPageBlob,
    pub write_position: usize,
    pub cache: Cache,
}

impl<TPageBlob: MyPageBlob> PageBlobSequenceWriter<TPageBlob> {
    pub fn new(src: PageBlobSequenceReaderWithCache<TPageBlob>) -> Self {
        Self {
            page_blob: src.page_blob,
            write_position: src.position,
            cache: Cache::new(BLOB_PAGE_SIZE, vec![], 0),
        }
    }

    fn get_position_to_write(&self) -> usize {
        //self.cache.
        0
    }

    pub fn append(&mut self, package: &mut PackageBuilder) {
        package.finalize();
        let buffer = &package.buffer;

        let position_to_write = self.get_position_to_write();

        self.cache.blob_is_increased(&buffer);

    }
}

#[cfg(test)]
mod tests {
    use my_azure_page_blob::MyPageBlobMock;

    #[test]
    fn test_positive_read_sequence() {
        let first_package = [1u8; 513];

        let my_page_blob = MyPageBlobMock::new();
    }
}
