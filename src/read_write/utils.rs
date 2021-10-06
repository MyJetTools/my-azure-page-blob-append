pub static END_MARKER: [u8; 4] = [0u8, 0u8, 0u8, 0u8];

pub fn get_position_within_page(page_blob_position: usize, page_size: usize) -> usize {
    let page_no = get_page_no_from_page_blob_position(page_blob_position, page_size);
    return page_blob_position - page_no * page_size;
}

pub fn get_page_no_from_page_blob_position(page_blob_position: usize, page_size: usize) -> usize {
    return page_blob_position / page_size;
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_page_blob_no_by_position() {
        assert_eq!(0, get_page_no_from_page_blob_position(1, 512));
        assert_eq!(0, get_page_no_from_page_blob_position(511, 512));

        assert_eq!(1, get_page_no_from_page_blob_position(512, 512));
        assert_eq!(1, get_page_no_from_page_blob_position(1023, 512));

        assert_eq!(2, get_page_no_from_page_blob_position(1024, 512));
    }

    //@todo - Debug
    #[test]
    fn test_position_within_page() {
        let result = get_position_within_page(0, 512);
        assert_eq!(0, result);
        let result = get_position_within_page(5, 512);
        assert_eq!(5, result);

        let result = get_position_within_page(512, 512);
        assert_eq!(0, result);
    }
}
