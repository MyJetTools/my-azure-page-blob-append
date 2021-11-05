#[derive(Clone, Copy)]
pub struct AppendPageBlobSettings {
    pub max_payload_size_protection: u32,
    pub blob_auto_resize_in_pages: usize,
    pub cache_capacity_in_pages: usize,
    pub max_pages_to_write_single_round_trip: usize,
}
