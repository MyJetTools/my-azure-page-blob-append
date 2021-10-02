pub struct Cache {
    data: Vec<u8>,
    position_in_blob: usize,
    position_in_last_pages: usize,
    page_size: usize,
}

impl Cache {
    pub fn new(page_size: usize, data: Vec<u8>, position_in_blob: usize) -> Self {
        let mut result = Self {
            data,
            position_in_blob,
            position_in_last_pages: 0,
            page_size,
        };

        result.position_in_last_pages = result.get_position_in_last_pages(position_in_blob);

        result
    }

    pub fn position_in_cache(&self) -> usize {
        return self.get_position_in_last_pages(self.position_in_blob);
    }

    pub fn blob_is_increased(&self, buffer: &[u8]) {
        let new_position = self.position_in_blob + buffer.len();

        let new_position_in_cache = self.get_position_in_last_pages(new_position);

        self.data = buffer[buffer.len() - self.page_size..].to_vec();
    }

    fn get_pages_offset(&self) -> usize{
        self.position_in_blob / self.

    }

    #[inline]
    fn get_position_in_last_pages(&self, position: usize) -> usize {
        return position - self.page_size * self.pages_offset;
    }
}
