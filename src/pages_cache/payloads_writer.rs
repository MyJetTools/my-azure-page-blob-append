use crate::PageCache;

pub struct PayloadsWriter<'s> {
    pages_cache: &'s mut PageCache,
    written: usize,
}

impl<'s> PayloadsWriter<'s> {
    pub fn new(src: &'s mut PageCache) -> Self {
        Self {
            pages_cache: src,
            written: 0,
        }
    }

    pub fn append_payload(&mut self, payload: &[u8]) {
        let size = payload.len() as i32;

        self.pages_cache
            .data
            .extend_from_slice(size.to_le_bytes().as_slice());
        self.pages_cache.data.extend_from_slice(payload);
        self.written += payload.len() + 4;
    }
}

impl<'s> Drop for PayloadsWriter<'s> {
    fn drop(&mut self) {
        self.pages_cache.advance_blob_position(self.written);
        let end = [0u8; 4];
        self.pages_cache.write(end.as_slice(), false);
    }
}
