pub struct ReadCache {
    buffer: Vec<u8>,
    read_position: usize,
    page_size: usize,
}

impl ReadCache {
    pub fn new(page_size: usize, pages: usize) -> Self {
        let result = Self {
            buffer: Vec::with_capacity(page_size * pages),
            read_position: 0,
            page_size,
        };
        result
    }

    pub fn get_last_page(&self) -> (usize, Option<Vec<u8>>) {
        let end_position = self.read_position - 4;
        let position_within_last_page =
            super::utils::get_position_within_page(end_position, self.page_size);

        let start_page_position =
            super::utils::get_page_no_from_page_blob_position(end_position, self.page_size)
                * self.page_size;

        if position_within_last_page == 0 {
            return (end_position, None);
        }

        let start_pos = self.buffer.len() - self.page_size;

        let result = &self.buffer[start_pos..start_pos + position_within_last_page];
        return (end_position, Some(result.to_vec()));
    }

    pub fn available_to_read_size(&self) -> usize {
        self.buffer.len() - self.read_position
    }

    pub fn upload(&mut self, buffer: &[u8]) {
        self.buffer.extend(buffer);
    }

    #[inline]
    fn advance_position(&mut self, size: usize) {
        self.read_position += size;
        if self.read_position == self.buffer.len() {
            self.buffer.clear();
            self.read_position = 0;
        }
    }

    pub fn copy_to(&mut self, data: &mut [u8]) -> usize {
        let max_to_copy = self.available_to_read_size();

        if data.len() <= max_to_copy {
            let src = &self.buffer[self.read_position..self.read_position + data.len()];
            data.copy_from_slice(src);
            self.advance_position(data.len());
            return data.len();
        }

        let dest_data = &mut data[..max_to_copy];

        let src = &self.buffer[self.read_position..self.read_position + max_to_copy];
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
        let mut buffer = ReadCache::new(8, 4);

        let src = [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(&src);

        let mut dest = [255u8, 255u8, 255u8];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 3);
        assert_eq!(dest, [0u8, 1u8, 2u8]);
        assert_eq!(buffer.read_position, 3);
    }

    #[test]
    fn test_if_we_have_not_enough_to_copy() {
        let mut buffer = ReadCache::new(8, 2);

        let src = [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(&src);

        let mut dest = [
            255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8,
        ];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 8);
        assert_eq!(dest, [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 255u8]);

        assert_eq!(buffer.read_position, 0);
    }

    #[test]
    fn test_if_we_have_exact_amount_to_copy() {
        let mut buffer = ReadCache::new(8, 2);

        let src = [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(&src);

        let mut dest = [255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 8);
        assert_eq!(dest, [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8]);

        assert_eq!(buffer.read_position, 0);
    }

    #[test]
    fn test_several_copy() {
        let mut buffer = ReadCache::new(8, 2);

        let src = [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(&src);

        let mut dest = [255u8, 255u8];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [0u8, 1u8]);

        assert_eq!(buffer.read_position, 2);

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [2u8, 3u8]);

        assert_eq!(buffer.read_position, 4);

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [4u8, 5u8]);

        assert_eq!(buffer.read_position, 6);

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [6u8, 7u8]);

        assert_eq!(buffer.read_position, 0);
    }
}
