use std::usize;

pub struct Cache {
    pub data: Vec<u8>,
    pub position_in_blob: usize,
    pub position_in_last_pages: usize,
    pub page_size: usize,
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

    pub fn blob_is_increased(&mut self, buffer: &[u8], position_increase: usize) {
        let new_position_in_blob = self.position_in_blob + position_increase;

        let new_position_in_cache = self.get_position_in_last_pages(new_position_in_blob);
        let buffer_length = buffer.len();
        let page_amount = buffer_length / self.page_size;

        if page_amount >= 2 {
            self.data = buffer[buffer.len() - self.page_size*2..].to_vec();
        } else {
            self.data = buffer[buffer.len() - self.page_size..].to_vec();
        }

        self.position_in_blob = new_position_in_blob;
        self.position_in_last_pages = new_position_in_cache;
    }

    /* fn get_pages_offset(&self) -> usize{
        let page_number = self.position_in_blob / self.page_size;
    } */

    #[inline]
    fn get_position_in_last_pages(&self, position: usize) -> usize {
        return position % self.page_size;
    }
}


#[cfg(test)]
mod tests {
    use super::Cache;

    #[test]
    fn test_1() {
        let mut data: Vec<u8> = vec![];

        for i in 1..=24  {
            data.push(i);
        }

        let cache = Cache::new(8, data, 0);
    }
}