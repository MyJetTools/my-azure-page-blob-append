use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

pub struct MyPageBlobWithCache<TPageBlob: MyPageBlob> {
    pub pages_amount: Option<usize>,
    pub page_blob: TPageBlob,
    max_pages_to_write_per_transaction: usize,
    resize_pages_ratio: usize,
}

impl<TPageBlob: MyPageBlob> MyPageBlobWithCache<TPageBlob> {
    pub fn new(
        page_blob: TPageBlob,
        max_pages_to_write_per_transaction: usize,
        resize_pages_ratio: usize,
    ) -> Self {
        Self {
            pages_amount: None,
            page_blob,
            max_pages_to_write_per_transaction,
            resize_pages_ratio,
        }
    }

    pub async fn init(&mut self) -> Result<(), AzureStorageError> {
        let pages_amount = super::get_available_pages_amount::with_retries(&self.page_blob).await?;
        self.pages_amount = Some(pages_amount);
        Ok(())
    }

    pub async fn try_get_pages_amount(&self) -> Result<usize, AzureStorageError> {
        match self.pages_amount {
            Some(result) => Ok(result),
            None => {
                let err = AzureStorageError::UnknownError {
                    msg: "PageBlobWithCache is not initialized".to_string(),
                };
                return Err(err);
            }
        }
    }

    pub async fn get_pages_amount(&mut self) -> Result<usize, AzureStorageError> {
        match self.pages_amount {
            Some(result) => Ok(result),
            None => {
                self.init().await?;
                return Ok(self.pages_amount.unwrap());
            }
        }
    }

    pub async fn create_blob_if_not_exists(
        &mut self,
        init_pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let size =
            super::create_blob_if_not_exists::with_retries(&self.page_blob, init_pages_amount)
                .await?;

        self.pages_amount = Some(size);
        return Ok(());
    }

    pub async fn resize_page_blob(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        super::resize_page_blob::with_retries(&self.page_blob, pages_amount).await?;
        self.pages_amount = Some(pages_amount);
        return Ok(());
    }

    pub async fn auto_resize_and_save_pages(
        &mut self,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let available_pages_amount = self.get_pages_amount().await?;
        let pages_amount_after_append = get_pages_amount_after_append(start_page_no, payload.len());

        if pages_amount_after_append > available_pages_amount {
            let has_to_have_pages_amount =
                get_ressize_to_pages_amount(pages_amount_after_append, self.resize_pages_ratio);

            self.resize_page_blob(has_to_have_pages_amount).await?;
        }

        let mut pages_to_write = payload.len() / BLOB_PAGE_SIZE;

        if pages_to_write <= self.max_pages_to_write_per_transaction {
            super::write_pages::with_retries(&self.page_blob, start_page_no, payload).await?;
            return Ok(());
        }

        let mut start_offset = 0;

        while pages_to_write > 0 {
            let now_writing_pages_amount =
                if pages_to_write <= self.max_pages_to_write_per_transaction {
                    pages_to_write
                } else {
                    self.max_pages_to_write_per_transaction
                };

            let now_writing_payload_size = now_writing_pages_amount * BLOB_PAGE_SIZE;

            let current_payload_to_write =
                &payload[start_offset..start_offset + now_writing_payload_size];

            super::write_pages::with_retries(
                &self.page_blob,
                start_page_no,
                current_payload_to_write.to_vec(),
            )
            .await?;

            start_offset += now_writing_payload_size;
            pages_to_write -= now_writing_pages_amount;
        }

        Ok(())
    }

    pub async fn read_pages(
        &self,
        start_page: usize,
        pages_to_read: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        super::read_pages::with_retries(&self.page_blob, start_page, pages_to_read).await
    }
}

pub fn get_pages_amount_after_append(start_page_no: usize, data_len: usize) -> usize {
    let data_len_in_pages = data_len / BLOB_PAGE_SIZE;
    return start_page_no + data_len_in_pages;
}

pub fn get_ressize_to_pages_amount(pages_amount_needs: usize, pages_resize_ratio: usize) -> usize {
    let full_pages_amount = (pages_amount_needs - 1) / pages_resize_ratio + 1;

    return full_pages_amount * pages_resize_ratio;
}

#[cfg(test)]
fn get_full_pages_size(len: usize) -> usize {
    let pages = (len - 1) / BLOB_PAGE_SIZE;

    (pages + 1) * BLOB_PAGE_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_full_page_ressize() {
        assert_eq!(512, get_full_pages_size(1));
        assert_eq!(512, get_full_pages_size(512));
        assert_eq!(1024, get_full_pages_size(513));
        assert_eq!(1024, get_full_pages_size(1024));
    }

    #[test]
    fn test_get_pages_amount_after_append() {
        assert_eq!(3, get_pages_amount_after_append(2, 512));
    }

    #[test]
    fn test_new_blob_size_in_pages_by_2() {
        let need_pages = 1;
        let pages_ratio = 2;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(2, ressize_to_pages_amount);

        let need_pages = 2;
        let pages_ratio = 2;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(2, ressize_to_pages_amount);

        let need_pages = 3;
        let pages_ratio = 2;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(4, ressize_to_pages_amount);

        let need_pages = 4;
        let pages_ratio = 2;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(4, ressize_to_pages_amount);
    }

    #[test]
    fn test_new_blob_size_in_pages_by_3() {
        let need_pages = 1;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(3, ressize_to_pages_amount);

        let need_pages = 2;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(3, ressize_to_pages_amount);

        let need_pages = 3;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(3, ressize_to_pages_amount);
        //

        let need_pages = 4;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(6, ressize_to_pages_amount);

        let need_pages = 5;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(6, ressize_to_pages_amount);

        let need_pages = 6;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(6, ressize_to_pages_amount);
    }
}
