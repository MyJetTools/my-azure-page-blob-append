use my_azure_page_blob::MyPageBlob;

use crate::{
    read_write::{PackageBuilder, PageBlobSequenceWriter},
    settings::AppendPageBlobSettings,
    PageBlobAppendCacheError, StateDataInitializing,
};

pub struct StateDataWriting<TMyPageBlob: MyPageBlob> {
    pub blob_position: usize,
    pub page_blob_seq_writer: PageBlobSequenceWriter<TMyPageBlob>,
}

impl<TMyPageBlob: MyPageBlob> StateDataWriting<TMyPageBlob> {
    pub fn from_initializing(
        src: StateDataInitializing<TMyPageBlob>,
        settings: &AppendPageBlobSettings,
    ) -> Self {
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
