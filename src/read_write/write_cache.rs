pub struct WriteCache {
    last_page: Option<Vec<u8>>,
    pub write_position: usize,
    page_size: usize,
    next_last_page: Option<Vec<u8>>,
    next_write_position: usize,
}

impl WriteCache {
    pub fn new(page_size: usize, last_page: Option<Vec<u8>>, write_position: usize) -> Self {
        Self {
            last_page,
            write_position,
            page_size,
            next_last_page: None,
            next_write_position: 0,
        }
    }

    pub fn concat_with_current_cache(&self, payload: &[u8]) -> Vec<u8> {
        let mut result = Vec::new();

        let position_within_page =
            super::utils::get_position_within_page(self.write_position, self.page_size);

        if position_within_page > 0 {
            if let Some(last_page) = &self.last_page {
                result.extend(&last_page[..position_within_page])
            }
        }

        result.extend(payload);

        result
    }

    //We adding last 2 or one pages closing with [0,0,0,0]
    pub fn start_increasing_blob(&mut self, buffer: &[u8]) {
        self.next_write_position = self.write_position + buffer.len() - 4;

        if let Some(last_page) = &self.last_page {
            self.next_write_position -= last_page.len();
        }

        let pos_within_last_page =
            super::utils::get_position_within_page(self.next_write_position, self.page_size);

        if pos_within_last_page == 0 {
            self.next_last_page = None;
        } else {
            let last_page_pos =
                super::utils::get_page_no_from_page_blob_position(buffer.len() - 4, self.page_size)
                    * self.page_size;

            self.next_last_page =
                Some(buffer[last_page_pos..last_page_pos + pos_within_last_page].to_vec());
        }
    }

    pub fn written(&mut self) {
        self.write_position = self.next_write_position;

        let mut last_page = None;

        std::mem::swap(&mut last_page, &mut self.next_last_page);

        self.last_page = last_page;
    }
}

#[cfg(test)]
mod tests {

    use crate::read_write::WriteCache;

    #[test]
    fn test_full_page_added_sequence_from_scratch() {
        let mut write_cache = WriteCache::new(8, None, 0);

        let mut package = vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8];

        package.extend([0u8, 0u8, 0u8, 0u8]);

        write_cache.start_increasing_blob(&package);

        assert_eq!(8, write_cache.next_write_position);
        assert_eq!(true, write_cache.next_last_page.is_none());
    }

    #[test]
    fn test_not_full_one_page_added_from_scratch() {
        let mut write_cache = WriteCache::new(8, None, 0);

        let mut package = vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];

        package.extend([0u8, 0u8, 0u8, 0u8]);

        write_cache.start_increasing_blob(&package);

        assert_eq!(7, write_cache.next_write_position);

        let next_last_page = write_cache.next_last_page.unwrap();

        assert_eq!(vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8], next_last_page);
    }

    #[test]
    fn test_not_full_two_pages_added_from_scratch() {
        let mut write_cache = WriteCache::new(8, None, 0);

        let mut package = vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8, 9u8];

        package.extend([0u8, 0u8, 0u8, 0u8]);

        write_cache.start_increasing_blob(&package);

        assert_eq!(9, write_cache.next_write_position);

        let next_last_page = write_cache.next_last_page.unwrap();

        assert_eq!(vec![9u8], next_last_page);
    }

    #[test]
    fn test_several_appends() {
        let mut write_cache = WriteCache::new(8, None, 0);

        let mut package = vec![1u8, 2u8, 3u8];
        package.extend([0u8, 0u8, 0u8, 0u8]);

        write_cache.start_increasing_blob(&package);

        let next_last_page = write_cache.next_last_page.as_ref().unwrap().to_vec();

        assert_eq!(vec![1u8, 2u8, 3u8], next_last_page);

        write_cache.written();

        // Adding new Package

        let mut package = vec![4u8, 5u8, 6u8];
        package.extend([0u8, 0u8, 0u8, 0u8]);

        let package_to_write = write_cache.concat_with_current_cache(&package);

        assert_eq!(
            vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 0u8, 0u8, 0u8, 0u8],
            package_to_write
        );

        write_cache.start_increasing_blob(&package_to_write);

        let next_last_page = write_cache.next_last_page.as_ref().unwrap().to_vec();

        assert_eq!(vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8], next_last_page);

        write_cache.written();

        // Adding new Package

        let mut package = vec![7u8, 8u8, 9u8];
        package.extend([0u8, 0u8, 0u8, 0u8]);

        let package_to_write = write_cache.concat_with_current_cache(&package);

        assert_eq!(
            vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8, 9u8, 0u8, 0u8, 0u8, 0u8],
            package_to_write
        );

        write_cache.start_increasing_blob(&package_to_write);

        let next_last_page = write_cache.next_last_page.as_ref().unwrap().to_vec();

        assert_eq!(9, write_cache.next_write_position);

        assert_eq!(vec![9u8], next_last_page);

        write_cache.written();

        // Adding new Package

        let mut package = vec![10u8, 11u8, 12u8];
        package.extend([0u8, 0u8, 0u8, 0u8]);

        let package_to_write = write_cache.concat_with_current_cache(&package);

        assert_eq!(
            vec![9u8, 10u8, 11u8, 12u8, 0u8, 0u8, 0u8, 0u8],
            package_to_write
        );

        write_cache.start_increasing_blob(&package_to_write);

        let next_last_page = write_cache.next_last_page.as_ref().unwrap().to_vec();

        assert_eq!(12, write_cache.next_write_position);

        assert_eq!(vec![9u8, 10u8, 11u8, 12u8], next_last_page);

        write_cache.written();

        // Adding new Package

        let mut package = vec![13u8, 14u8, 15u8];
        package.extend([0u8, 0u8, 0u8, 0u8]);

        let package_to_write = write_cache.concat_with_current_cache(&package);

        assert_eq!(
            vec![9u8, 10u8, 11u8, 12u8, 13u8, 14u8, 15u8, 0u8, 0u8, 0u8, 0u8],
            package_to_write
        );

        write_cache.start_increasing_blob(&package_to_write);

        let next_last_page = write_cache.next_last_page.as_ref().unwrap().to_vec();

        assert_eq!(15, write_cache.next_write_position);

        assert_eq!(
            vec![9u8, 10u8, 11u8, 12u8, 13u8, 14u8, 15u8],
            next_last_page
        );

        write_cache.written();

        // Adding new Package

        let mut package = vec![16u8, 17u8, 18u8];
        package.extend([0u8, 0u8, 0u8, 0u8]);

        let package_to_write = write_cache.concat_with_current_cache(&package);

        assert_eq!(
            vec![9u8, 10u8, 11u8, 12u8, 13u8, 14u8, 15u8, 16u8, 17u8, 18u8, 0u8, 0u8, 0u8, 0u8],
            package_to_write
        );

        write_cache.start_increasing_blob(&package_to_write);

        let next_last_page = write_cache.next_last_page.as_ref().unwrap().to_vec();

        assert_eq!(18, write_cache.next_write_position);

        assert_eq!(vec![17u8, 18u8], next_last_page);

        write_cache.written();

        // Adding new Package

        let mut package = vec![19u8, 20u8, 21u8, 22u8, 23u8, 24u8];
        package.extend([0u8, 0u8, 0u8, 0u8]);

        let package_to_write = write_cache.concat_with_current_cache(&package);

        assert_eq!(
            vec![17u8, 18u8, 19u8, 20u8, 21u8, 22u8, 23u8, 24u8, 0u8, 0u8, 0u8, 0u8],
            package_to_write
        );

        write_cache.start_increasing_blob(&package_to_write);

        assert_eq!(24, write_cache.next_write_position);

        assert_eq!(true, write_cache.next_last_page.is_none());

        write_cache.written();
    }
}
