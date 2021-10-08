pub struct ReadCache {
    buffer: Option<Vec<u8>>,
    prev_buffer_last_page: Option<Vec<u8>>,
    read_position: usize,
    page_size: usize,
    pub read_blob_position: usize,
    first_page_no: usize,
    pages_in_buffer: usize,
}

impl ReadCache {
    pub fn new(page_size: usize) -> Self {
        let result = Self {
            buffer: None,
            prev_buffer_last_page: None,
            read_position: 0,
            read_blob_position: 0,
            page_size,
            first_page_no: 0,
            pages_in_buffer: 0,
        };
        result
    }

    pub fn get_page_from_buffer(&self, negative_offset: usize) -> &[u8] {
        let buffer = self.buffer.as_ref().unwrap();

        let page_no = super::utils::get_page_no_from_page_blob_position(
            self.read_blob_position - negative_offset,
            self.page_size,
        );

        if let Some(prev_buffer) = &self.prev_buffer_last_page {
            if page_no == self.first_page_no - 1 {
                return prev_buffer;
            }

            if page_no < self.first_page_no - 1 {
                panic!(
                    "Somehow we requested page_no {}. first_page_no is {}",
                    page_no, self.first_page_no
                );
            }
        }

        let page_no_in_buffer = page_no - self.first_page_no;
        let buffer_offset = page_no_in_buffer * self.page_size;
        return &buffer[buffer_offset..buffer_offset + self.page_size];
    }

    pub fn get_last_page_remaining_content(
        &mut self,
        negative_offset: usize,
    ) -> (usize, Option<Vec<u8>>) {
        if self.read_blob_position == 0 {
            return (0, None);
        }

        let read_blob_position = self.read_blob_position - negative_offset;

        let position_within_last_page =
            super::utils::get_position_within_page(read_blob_position, self.page_size);

        if position_within_last_page == 0 {
            return (read_blob_position, None);
        }

        let last_page = self.get_page_from_buffer(negative_offset);

        return (
            read_blob_position,
            Some(last_page[..position_within_last_page].to_vec()),
        );
    }

    pub fn available_to_read_size(&self) -> usize {
        if self.buffer.is_none() {
            return 0;
        }

        let buffer_size = self.buffer.as_ref().unwrap().len();

        return buffer_size - self.read_position;
    }

    pub fn upload(&mut self, buffer: Vec<u8>) {
        if buffer.len() % self.page_size != 0 {
            panic!(
                "Invalid buffer size {}. It has to be Modular to {}",
                buffer.len(),
                self.page_size
            );
        }

        if self.buffer.is_some() {
            panic!("We can upload to the buffer only when it empty");
        }

        self.first_page_no += self.pages_in_buffer;
        self.pages_in_buffer = buffer.len() / self.page_size;

        self.buffer = Some(buffer);
    }

    #[inline]
    fn advance_position(&mut self, size: usize) {
        if self.buffer.is_none() {
            panic!("We can not advance position at Empty Buffer");
        }

        let buffer_size = self.buffer.as_ref().unwrap().len();

        self.read_blob_position += size;
        self.read_position += size;
        if self.read_position == buffer_size {
            self.prev_buffer_last_page =
                Some(self.buffer.as_ref().unwrap()[buffer_size - self.page_size..].to_vec());

            self.buffer = None;
            self.read_position = 0;
        }
    }

    pub fn copy_to(&mut self, data: &mut [u8]) -> usize {
        if self.buffer.is_none() {
            panic!("We can not read from Empty Buffer");
        }

        let buffer = self.buffer.as_ref().unwrap();

        let max_to_copy = self.available_to_read_size();

        if data.len() <= max_to_copy {
            let src = &buffer[self.read_position..self.read_position + data.len()];
            data.copy_from_slice(src);
            self.advance_position(data.len());
            return data.len();
        }

        let dest_data = &mut data[..max_to_copy];

        let src = &buffer[self.read_position..self.read_position + max_to_copy];
        dest_data.copy_from_slice(src);

        self.advance_position(max_to_copy);
        return max_to_copy;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_if_we_have_enough_to_copy() {
        let mut buffer = ReadCache::new(8);

        let src = vec![0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(src);

        let mut dest = [255u8, 255u8, 255u8];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 3);
        assert_eq!(dest, [0u8, 1u8, 2u8]);
        assert_eq!(buffer.read_position, 3);
    }

    #[test]
    fn test_if_we_have_not_enough_to_copy() {
        let mut buffer = ReadCache::new(8);

        let src = vec![0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(src);

        let mut dest = [
            255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8,
        ];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 8);
        assert_eq!(dest, [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 255u8]);

        assert_eq!(buffer.read_position, 0);
        assert_eq!(buffer.read_blob_position, 8);
    }

    #[test]
    fn test_if_we_have_exact_amount_to_copy() {
        let mut buffer = ReadCache::new(8);

        let src = vec![0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(src);

        let mut dest = [255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 8);
        assert_eq!(dest, [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8]);

        assert_eq!(buffer.read_position, 0);
    }

    #[test]
    fn test_several_copy() {
        let mut buffer = ReadCache::new(8);

        let src = vec![0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(src);

        let mut dest = [255u8, 255u8];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [0u8, 1u8]);

        assert_eq!(buffer.read_position, 2);
        assert_eq!(buffer.read_blob_position, 2);

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [2u8, 3u8]);

        assert_eq!(buffer.read_position, 4);
        assert_eq!(buffer.read_blob_position, 4);

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [4u8, 5u8]);

        assert_eq!(buffer.read_position, 6);
        assert_eq!(buffer.read_blob_position, 6);

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [6u8, 7u8]);

        assert_eq!(buffer.read_position, 0);
        assert_eq!(buffer.read_blob_position, 8);
    }

    #[test]
    fn test_remaining_conten_on_previous_payload() {
        let mut reade_cache = ReadCache::new(4);

        let src = vec![0u8, 1u8, 2u8, 3u8];
        reade_cache.upload(src);

        let mut download_buffer = [0u8; 4];

        reade_cache.copy_to(&mut download_buffer);

        let src = vec![5u8, 6u8, 7u8, 8u8];
        reade_cache.upload(src);

        let mut download_buffer = [0u8; 2];

        reade_cache.copy_to(&mut download_buffer);

        let (pos, remaining) = reade_cache.get_last_page_remaining_content(4);

        assert_eq!(2, pos);

        assert_eq!(vec![0u8, 1u8], remaining.unwrap());
    }
}
