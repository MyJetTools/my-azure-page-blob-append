use my_azure_page_blob::MyPageBlob;

use crate::{
    read_write::{PackageBuilder, PageBlobSequenceWriter},
    settings::AppendPageBlobSettings,
    PageBlobAppendError,
};

use super::StateDataReading;

pub struct StateDataWriting<TMyPageBlob: MyPageBlob> {
    pub seq_writer: PageBlobSequenceWriter<TMyPageBlob>,
}

impl<TMyPageBlob: MyPageBlob> StateDataWriting<TMyPageBlob> {
    pub fn from_initializing(
        src: StateDataReading<TMyPageBlob>,
        settings: &AppendPageBlobSettings,
    ) -> Self {
        Self {
            seq_writer: PageBlobSequenceWriter::new(src.seq_reader, settings),
        }
    }

    pub fn get_blob_position(&self) -> usize {
        self.seq_writer.write_cache.write_position
    }

    pub async fn append_and_write<'s>(
        &mut self,
        payloads: &Vec<Vec<u8>>,
    ) -> Result<(), PageBlobAppendError> {
        let mut builder = PackageBuilder::new();

        for payload in payloads {
            builder.add_payload(payload);
        }

        self.seq_writer.append(builder).await?;

        Ok(())
    }
}
