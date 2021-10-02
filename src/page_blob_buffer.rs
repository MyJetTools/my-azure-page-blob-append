pub struct PageBlobBuffer {
    buffer: Vec<u8>,
    position: usize,
}

impl PageBlobBuffer {
    pub fn new(capacity_size: usize) -> Self {
        let result = Self {
            buffer: Vec::with_capacity(capacity_size),
            position: 0,
        };
        result
    }

    pub fn available_to_read_size(&self) -> usize {
        self.buffer.len() - self.position
    }

    pub fn upload(&mut self, buffer: &[u8]) {
        self.buffer.extend(buffer);
    }

    #[inline]
    fn advance_position(&mut self, size: usize) {
        self.position += size;
        if self.position == self.buffer.len() {
            self.buffer.clear();
            self.position = 0;
        }
    }

    pub fn copy_to(&mut self, data: &mut [u8]) -> usize {
        let max_to_copy = self.available_to_read_size();

        if data.len() <= max_to_copy {
            let src = &self.buffer[self.position..self.position + data.len()];
            data.copy_from_slice(src);
            self.advance_position(data.len());
            return data.len();
        }

        let dest_data = &mut data[..max_to_copy];

        let src = &self.buffer[self.position..self.position + max_to_copy];
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
        let mut buffer = PageBlobBuffer::new(8);

        let src = [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(&src);

        let mut dest = [255u8, 255u8, 255u8];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 3);
        assert_eq!(dest, [0u8, 1u8, 2u8]);
        assert_eq!(buffer.position, 3);
    }

    #[test]
    fn test_if_we_have_not_enough_to_copy() {
        let mut buffer = PageBlobBuffer::new(8);

        let src = [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(&src);

        let mut dest = [
            255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8,
        ];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 8);
        assert_eq!(dest, [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 255u8]);

        assert_eq!(buffer.position, 0);
    }

    #[test]
    fn test_if_we_have_exact_amount_to_copy() {
        let mut buffer = PageBlobBuffer::new(8);

        let src = [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(&src);

        let mut dest = [255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 8);
        assert_eq!(dest, [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8]);

        assert_eq!(buffer.position, 0);
    }

    #[test]
    fn test_several_copy() {
        let mut buffer = PageBlobBuffer::new(8);

        let src = [0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        buffer.upload(&src);

        let mut dest = [255u8, 255u8];

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [0u8, 1u8]);

        assert_eq!(buffer.position, 2);

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [2u8, 3u8]);

        assert_eq!(buffer.position, 4);

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [4u8, 5u8]);

        assert_eq!(buffer.position, 6);

        let copied = buffer.copy_to(&mut dest);

        assert_eq!(copied, 2);
        assert_eq!(dest, [6u8, 7u8]);

        assert_eq!(buffer.position, 0);
    }
}
