pub fn get_pages_amount_by_size(data_size: usize, page_size: usize) -> usize {
    return (data_size - 1) / page_size + 1;
}

#[cfg(test)]
pub fn get_pages_amount_by_size_including_buffer_capacity(
    data_size: usize,
    buffer_size: usize,
    page_size: usize,
) -> usize {
    let data_pages_amount = get_pages_amount_by_size(data_size, page_size);

    let buffer_pages = get_pages_amount_by_size(buffer_size, page_size);

    data_pages_amount + buffer_pages - 1
}

#[cfg(test)]
pub fn get_page_no_from_page_blob_position(page_blob_position: usize, page_size: usize) -> usize {
    return page_blob_position / page_size;
}

pub fn extend_buffer_to_full_pages_size(buffer: &mut Vec<u8>, page_size: usize) {
    let pages = get_pages_amount_by_size(buffer.len(), page_size);

    let full_size = pages * page_size;

    let full_size = full_size as usize;

    if full_size == buffer.len() {
        return;
    }

    let remains = full_size - buffer.len();

    for _ in 0..remains {
        buffer.push(0);
    }
}

#[cfg(test)]
pub fn get_last_page<'t>(data: &'t Vec<u8>, page_size: usize) -> &'t [u8] {
    let page_no = get_page_no_from_page_blob_position(data.len(), page_size);

    let start_page_position = page_no * page_size;

    return &data[start_page_position..];
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_get_pages_amount_by_size() {
        assert_eq!(1, get_pages_amount_by_size(1, 512));

        assert_eq!(1, get_pages_amount_by_size(512, 512));
        assert_eq!(2, get_pages_amount_by_size(513, 512));
        assert_eq!(2, get_pages_amount_by_size(1024, 512));

        assert_eq!(3, get_pages_amount_by_size(1025, 512));
        assert_eq!(3, get_pages_amount_by_size(1536, 512));
    }

    #[test]
    fn test_page_blob_no_by_position() {
        assert_eq!(0, get_page_no_from_page_blob_position(1, 512));
        assert_eq!(0, get_page_no_from_page_blob_position(511, 512));

        assert_eq!(1, get_page_no_from_page_blob_position(512, 512));
        assert_eq!(1, get_page_no_from_page_blob_position(1023, 512));

        assert_eq!(2, get_page_no_from_page_blob_position(1024, 512));
    }

    //@note - Debug
    #[test]
    fn test_donwload_pages_with_pages_capacity() {
        let data_size = 500;
        let buffer_size = 512;

        assert_eq!(
            1,
            get_pages_amount_by_size_including_buffer_capacity(data_size, buffer_size, 512)
        );

        let data_size = 512;
        let buffer_size = 512;

        assert_eq!(
            1,
            get_pages_amount_by_size_including_buffer_capacity(data_size, buffer_size, 512)
        );

        let data_size = 513;
        let buffer_size = 512;

        assert_eq!(
            2,
            get_pages_amount_by_size_including_buffer_capacity(data_size, buffer_size, 512)
        );
    }

    #[test]
    fn test_extend_buffer_to_full_page() {
        let mut buffer: Vec<u8> = Vec::new();

        buffer.push(15);
        buffer.push(16);

        extend_buffer_to_full_pages_size(&mut buffer, 512);

        assert_eq!(512, buffer.len());
    }
}
