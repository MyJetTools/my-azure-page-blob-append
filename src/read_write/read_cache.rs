pub struct ReadCache {
    buffer: Option<Vec<u8>>,
    last_page: Option<Vec<u8>>,
    read_position: usize,
    page_size: usize,
    read_blob_position: usize,
}

impl ReadCache {
    pub fn new(page_size: usize) -> Self {
        let result = Self {
            buffer: None,
            read_position: 0,
            read_blob_position: 0,
            page_size,
            last_page: None,
        };
        result
    }

    pub fn get_last_page(&mut self) -> (usize, Option<Vec<u8>>) {
        if self.read_blob_position == 0 {
            return (0, None);
        }

        let read_blob_position = self.read_blob_position - super::utils::END_MARKER.len();

        let position_within_last_page =
            super::utils::get_position_within_page(read_blob_position, self.page_size);

        if position_within_last_page == 0 {
            return (read_blob_position, None);
        }

        let last_page = &self.last_page.as_ref().unwrap()[..position_within_last_page];

        return (read_blob_position, Some(last_page.to_vec()));
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
        let last_page = &buffer[buffer.len() - self.page_size..];
        self.last_page = Some(last_page.to_vec());
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

    #[test]
    fn test_get_last_position() {
        let mut read_cache = ReadCache::new(8);

        let src = vec![
            0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8, 9u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
        ];
        read_cache.upload(src);

        let mut buffer = [0u8; 14];

        read_cache.copy_to(&mut buffer);

        let (blob_position, last_page) = read_cache.get_last_page();

        assert_eq!(10, blob_position);
        assert_eq!(vec![8u8, 9u8], last_page.unwrap());
    }
}
