use my_azure_page_blob::MyPageBlob;

use crate::{
    read_write::{PackageBuilder, PageBlobSequenceWriter},
    settings::Settings,
    PageBlobAppendCacheError, StateDataInitializing,
};

pub struct StateDataWriting<TMyPageBlob: MyPageBlob> {
    pub blob_position: usize,
    pub page_blob_seq_writer: PageBlobSequenceWriter<TMyPageBlob>,
}

impl<TMyPageBlob: MyPageBlob> StateDataWriting<TMyPageBlob> {
    pub fn from_initializing(src: StateDataInitializing<TMyPageBlob>, settings: &Settings) -> Self {
        Self {
            blob_position: src.blob_position,
            page_blob_seq_writer: PageBlobSequenceWriter::new(src.seq_reader, settings),
        }
    }

    pub async fn append_and_write<'s>(
        &mut self,
        payloads: &Vec<Vec<u8>>,
    ) -> Result<(), PageBlobAppendCacheError> {
        let mut builder = PackageBuilder::new();

        for payload in payloads {
            builder.add_payload(payload);
        }

        self.page_blob_seq_writer.append(builder).await?;

        Ok(())
    }
}

/*

#[cfg(test)]
mod tests {

    use my_azure_page_blob::*;

    use super::*;

    fn assert_bytes(left: &[u8], right: &[u8]) {
        assert_eq!(left.len(), right.len());

        for i in 0..left.len() {
            let left_b = left[i];
            let right_b = right[i];

            if left_b != right_b {
                println!("Not the same numbers at position: {}", i);
            }
            assert_eq!(left_b, right_b);
        }
    }

    fn compile_payload(b: u8, amount: usize) -> Vec<u8> {
        let mut result = Vec::with_capacity(amount);
        for _ in 0..amount {
            result.push(b);
        }

        result
    }

    impl PageBlobAppendCache {
        pub async fn init_for_tests<T: MyPageBlob>(&mut self, page_blob: &mut T) {
            loop {
                let msg = self.get_next_payload(page_blob).await;

                if let Err(err) = &msg {
                    if let PageBlobAppendCacheError::AzureStorageError(err) = err {
                        if let AzureStorageError::BlobNotFound = err {
                            page_blob.create_if_not_exists(0).await.unwrap();
                            return;
                        }
                    }
                }

                let msg = msg.unwrap();

                if msg.is_none() {
                    break;
                }
            }
        }
    }


impl AppendCacheState {
    pub fn new_initializing(blob_size_in_pages: usize) -> Self {
        Self::Initializing(BlobData { blob_size_in_pages })
    }

    pub fn new_initialized(blob_size_in_pages: usize) -> Self {
        Self::Initialized(BlobData { blob_size_in_pages })
    }

    pub fn is_initialized(&self) -> bool {
        match self {
            AppendCacheState::Initialized(_) => true,
            _ => false,
        }
    }

    pub fn get_size_for_write_purposes(&self) -> Result<usize, PageBlobAppendCacheError> {
        match self {
            AppendCacheState::NotInitialized => {
                return Err(PageBlobAppendCacheError::NotInitialized);
            }
            AppendCacheState::Initializing(state) => {
                return Err(PageBlobAppendCacheError::NotInitialized);
            }
            AppendCacheState::Corrupted => {
                return Err(PageBlobAppendCacheError::Corrupted);
            }
            AppendCacheState::Initialized(state) => {
                return Ok(state.blob_size_in_pages);
            }
        }
    }
}





#[cfg(test)]
mod tests {

    use my_azure_page_blob::*;

    use super::*;

    fn assert_bytes(left: &[u8], right: &[u8]) {
        assert_eq!(left.len(), right.len());

        for i in 0..left.len() {
            let left_b = left[i];
            let right_b = right[i];

            if left_b != right_b {
                println!("Not the same numbers at position: {}", i);
            }
            assert_eq!(left_b, right_b);
        }
    }

    fn compile_payload(b: u8, amount: usize) -> Vec<u8> {
        let mut result = Vec::with_capacity(amount);
        for _ in 0..amount {
            result.push(b);
        }

        result
    }

    impl PageBlobAppendCache {
        pub async fn init_for_tests<T: MyPageBlob>(&mut self, page_blob: &mut T) {
            loop {
                let msg = self.get_next_payload(page_blob).await;

                if let Err(err) = &msg {
                    if let PageBlobAppendCacheError::AzureStorageError(err) = err {
                        if let AzureStorageError::BlobNotFound = err {
                            page_blob.create_if_not_exists(0).await.unwrap();
                            return;
                        }
                    }
                }

                let msg = msg.unwrap();

                if msg.is_none() {
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_append_cases() {
        let mut page_blob = MyPageBlobMock::new();

        let mut append_cache = PageBlobAppendCache::new(8, 8, 10, true);

        append_cache.init_for_tests(&mut page_blob).await;

        let mut payloads = Vec::new();

        payloads.push(vec![5, 5, 5]);

        append_cache
            .append_and_write(&mut page_blob, &payloads, 100)
            .await
            .unwrap();

        let result_buffer = page_blob.download().await.unwrap();

        assert_bytes(
            &result_buffer[0..11],
            &vec![3u8, 0u8, 0u8, 0u8, 5u8, 5u8, 5u8, 0u8, 0u8, 0u8, 0u8],
        );

        let mut payloads = Vec::new();

        payloads.push(vec![6, 6, 6]);

        append_cache
            .append_and_write(&mut page_blob, &payloads, 100)
            .await
            .unwrap();

        let result_buffer = page_blob.download().await.unwrap();

        println!("{:?}", &result_buffer[0..18]);

        assert_bytes(
            &result_buffer[0..18],
            &vec![
                3u8, 0u8, 0u8, 0u8, 5u8, 5u8, 5u8, //First message
                3u8, 0u8, 0u8, 0u8, 6u8, 6u8, 6u8, //Second Message
                0u8, 0u8, 0u8, 0u8, // The end
            ],
        );
    }

    #[tokio::test]
    async fn test_init_pages_on_brand_new_page() {
        let first_payload = compile_payload(5, 513);
        let second_payload = compile_payload(6, 513);

        test_with_two_payloads_script(first_payload.as_slice(), second_payload.as_slice(), 8).await;
    }

    #[tokio::test]
    async fn test_init_pages_than_last_page_fits_512() {
        let first_payload = compile_payload(5, 512 - 4);
        let second_payload = compile_payload(6, 512 - 4);
        test_with_two_payloads_script(first_payload.as_slice(), second_payload.as_slice(), 8).await;
    }

    #[tokio::test]
    async fn test_page_is_beond_of_auto_resize_buffer() {
        let first_payload = compile_payload(5, 1024);
        let second_payload = compile_payload(6, 1024);
        test_with_two_payloads_script(first_payload.as_slice(), second_payload.as_slice(), 2).await;
    }

    async fn test_with_two_payloads_script(
        first_payload: &[u8],
        second_payload: &[u8],
        blob_auto_resize_in_pages: usize,
    ) {
        let mut page_blob = MyPageBlobMock::new();

        let mut append_cache = PageBlobAppendCache::new(8, blob_auto_resize_in_pages, 1024, true);

        append_cache.init_for_tests(&mut page_blob).await;

        let mut payloads = Vec::new();

        payloads.push(first_payload.to_vec());

        append_cache
            .append_and_write(&mut page_blob, &payloads, 100)
            .await
            .unwrap();

        // Load from Blob and Append

        let mut append_cache = PageBlobAppendCache::new(8, blob_auto_resize_in_pages, 1024, true);

        append_cache.init_for_tests(&mut page_blob).await;

        let mut payloads = Vec::new();

        payloads.push(second_payload.to_vec());

        append_cache
            .append_and_write(&mut page_blob, &payloads, 100)
            .await
            .unwrap();

        let buffer_from_blob = page_blob.download().await.unwrap();

        let mut result: Vec<u8> = Vec::new();

        let first_len = (first_payload.len() as i32).to_le_bytes();
        let second_len = (second_payload.len() as i32).to_le_bytes();

        result.extend(&first_len);
        result.extend(first_payload);
        result.extend(&second_len);
        result.extend(second_payload);
        result.extend(&[0u8, 0u8, 0u8, 0u8]);

        assert_bytes(
            result.as_slice(),
            &buffer_from_blob[..first_payload.len() + second_payload.len() + 12],
        );
    }
}
 */
