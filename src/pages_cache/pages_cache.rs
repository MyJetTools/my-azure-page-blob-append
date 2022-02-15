use crate::{page_blob_utils::*, PayloadsWriter};

pub struct PageCache {
    pub data: Vec<u8>,
    page_id_offset: usize,
    blob_position: usize,
    page_size: usize,
}

impl PageCache {
    pub fn new(
        data: Vec<u8>,
        page_id_offset: usize,
        blob_position: usize,
        page_size: usize,
    ) -> Self {
        Self {
            data,
            page_id_offset,
            blob_position,
            page_size,
        }
    }

    pub fn get_page_id_offset(&self) -> usize {
        self.page_id_offset
    }

    pub fn get_next_page_after_cache(&self) -> usize {
        let pages_in_cache = self.data.len() / self.page_size;
        self.page_id_offset + pages_in_cache
    }

    pub fn get_blob_position(&self) -> usize {
        self.blob_position
    }

    fn get_position_in_cache(&self) -> usize {
        self.blob_position - self.page_id_offset * self.page_size
    }

    pub fn append_payload_from_blob(&mut self, payload: &[u8]) {
        self.data.extend_from_slice(payload);
    }

    fn get_pages_in_cache(&self) -> usize {
        self.data.len() / self.page_size
    }

    pub fn gc(&mut self, keep_pages: usize) {
        let position_in_cache = self.get_position_in_cache();

        if position_in_cache < self.page_size {
            return;
        }

        let pages_in_cache = self.get_pages_in_cache();

        let mut pages_to_gc = position_in_cache / self.page_size;

        while pages_in_cache - pages_to_gc < keep_pages {
            if pages_to_gc == 0 {
                return;
            }
            pages_to_gc -= 1;
        }

        self.data.drain(..pages_to_gc * self.page_size);
        self.page_id_offset += pages_to_gc;
    }

    pub fn try_to_get_next_slice<'s>(&'s mut self, size: usize) -> Result<&'s [u8], usize> {
        let position_in_cache = self.get_position_in_cache();

        if position_in_cache + size > self.data.len() {
            return Err(position_in_cache + size - self.data.len());
        }

        Ok(&self.data[position_in_cache..position_in_cache + size])
    }

    fn prepare_to_write(&mut self) {
        let position_in_cache = self.get_position_in_cache();
        self.data.drain(position_in_cache..);
    }

    pub fn write(&mut self, payload: &[u8], advance_blob_position: bool) {
        self.prepare_to_write();
        self.data.extend_from_slice(payload);

        extend_buffer_to_full_pages_size(&mut self.data, self.page_size);

        if advance_blob_position {
            self.blob_position += payload.len();
        }
    }

    pub fn start_writing<'s>(&'s mut self) -> PayloadsWriter<'s> {
        self.prepare_to_write();
        return PayloadsWriter::new(self);
    }

    pub fn get_payload<'s>(&'s mut self) -> &'s [u8] {
        self.data.as_slice()
    }

    pub fn advance_blob_position(&mut self, delta: usize) {
        self.blob_position += delta;
    }

    pub fn reset_from_current_position(&mut self) {
        let position_in_cache = self.get_position_in_cache();
        for i in position_in_cache..self.data.len() {
            self.data[i] = 0;
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn generate_test_array(size: usize) -> Vec<u8> {
        let mut result = Vec::new();
        for i in 0..size {
            result.push(i as u8);
        }

        result
    }

    #[test]
    fn test_next_page_no_to_load() {
        const PAGE_SIZE: usize = 8;
        let mut read_cache = PageCache::new(vec![], 0, 0, PAGE_SIZE);

        assert_eq!(0, read_cache.get_next_page_after_cache());

        let page_to_upload = [0u8; PAGE_SIZE * 2];

        read_cache.append_payload_from_blob(page_to_upload.as_slice());

        assert_eq!(2, read_cache.get_next_page_after_cache());
    }

    #[test]
    fn test_we_reading_data_from_empty_cache() {
        const PAGE_SIZE: usize = 8;

        let mut read_cache = PageCache::new(vec![], 0, 0, PAGE_SIZE);

        match read_cache.try_to_get_next_slice(4) {
            Ok(_) => {
                panic!("Should not be here")
            }
            Err(size) => {
                assert_eq!(4, size);
            }
        }
    }

    #[test]
    fn test_we_reading_data_several_cases() {
        const PAGE_SIZE: usize = 8;
        let mut read_cache = PageCache::new(vec![], 0, 0, PAGE_SIZE);
        let page_to_upload = generate_test_array(PAGE_SIZE);
        read_cache.append_payload_from_blob(page_to_upload.as_slice());

        {
            let slice = read_cache.try_to_get_next_slice(4).unwrap();
            assert_eq!(vec![0u8, 1u8, 2u8, 3u8], slice);
        }

        read_cache.advance_blob_position(4);

        {
            let slice = read_cache.try_to_get_next_slice(2).unwrap();
            assert_eq!(vec![4u8, 5u8], slice);
        }

        read_cache.advance_blob_position(2);

        match read_cache.try_to_get_next_slice(8) {
            Ok(_) => {
                panic!("Should not be here")
            }
            Err(size) => {
                assert_eq!(6, size);
            }
        }
    }

    #[test]
    pub fn test_gc_read_cache() {
        const PAGE_SIZE: usize = 8;
        let mut read_cache = PageCache::new(vec![], 0, 0, PAGE_SIZE);
        let page_to_upload = generate_test_array(PAGE_SIZE * 3);

        read_cache.append_payload_from_blob(page_to_upload.as_slice());

        read_cache.try_to_get_next_slice(10).unwrap();
        read_cache.advance_blob_position(10);

        match read_cache.try_to_get_next_slice(24) {
            Ok(_) => {
                panic!("Should not be here")
            }
            Err(size) => {
                println!("Remaining size {}", size);
            }
        }

        assert_eq!(0, read_cache.page_id_offset);

        read_cache.gc(2);

        assert_eq!(1, read_cache.page_id_offset);

        assert_eq!(2, read_cache.get_position_in_cache());
        assert_eq!(10, read_cache.blob_position);
    }

    #[test]
    pub fn test_gc_when_we_have_alot_of_pages() {
        const PAGE_SIZE: usize = 8;
        let mut read_cache = PageCache::new(vec![], 0, 0, PAGE_SIZE);
        let page_to_upload = generate_test_array(PAGE_SIZE * 20);

        read_cache.append_payload_from_blob(page_to_upload.as_slice());

        read_cache.try_to_get_next_slice(155).unwrap();
        read_cache.advance_blob_position(155);

        match read_cache.try_to_get_next_slice(24) {
            Ok(_) => {
                panic!("Should not be here")
            }
            Err(size) => {
                println!("Remaining size {}", size);
            }
        }

        read_cache.gc(2);
        assert_eq!(18, read_cache.page_id_offset);
        assert_eq!(11, read_cache.get_position_in_cache());
        assert_eq!(155, read_cache.blob_position);

        let result = read_cache.try_to_get_next_slice(4).unwrap();

        assert_eq!(vec![155, 156, 157, 158], result);
    }

    #[test]
    pub fn test_add_new_content() {
        const PAGE_SIZE: usize = 8;
        let mut pages_cache = PageCache::new(vec![], 0, 0, PAGE_SIZE);

        pages_cache.write(vec![1u8, 1u8, 1u8, 1u8].as_slice(), true);

        assert_eq!(
            vec![1u8, 1u8, 1u8, 1u8, 0u8, 0u8, 0u8, 0u8],
            pages_cache.get_payload()
        );

        pages_cache.write(vec![2u8, 2u8, 2u8, 2u8].as_slice(), true);

        assert_eq!(
            vec![1u8, 1u8, 1u8, 1u8, 2u8, 2u8, 2u8, 2u8],
            pages_cache.get_payload()
        );

        pages_cache.write(vec![3u8, 3u8, 3u8, 3u8, 3u8].as_slice(), true);

        assert_eq!(
            vec![1u8, 1u8, 1u8, 1u8, 2u8, 2u8, 2u8, 2u8, 3u8, 3u8, 3u8, 3u8, 3u8, 0u8, 0u8, 0u8],
            pages_cache.get_payload()
        );

        pages_cache.write(vec![4u8, 4u8, 4u8, 4u8].as_slice(), true);

        assert_eq!(
            vec![
                1u8, 1u8, 1u8, 1u8, 2u8, 2u8, 2u8, 2u8, //Page 1
                3u8, 3u8, 3u8, 3u8, 3u8, 4u8, 4u8, 4u8, //Page 2
                4u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8 //Page 3
            ],
            pages_cache.get_payload()
        );
    }
}
