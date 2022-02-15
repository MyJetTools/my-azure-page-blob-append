mod blob_cache;
mod copy_blob;
pub mod create_blob_if_not_exists;
pub mod create_container_if_not_exist;
pub mod get_available_pages_amount;
pub mod read_pages;
pub mod resize_page_blob;
pub mod write_pages;

pub use blob_cache::MyPageBlobWithCache;
pub use copy_blob::copy_blob;
