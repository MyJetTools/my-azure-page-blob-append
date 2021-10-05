pub struct PackageBuilder {
    pub buffer: Vec<u8>,
}

impl PackageBuilder {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn add_payload(&mut self, payload: &[u8]) {
        let size = payload.len() as i32;

        let size_as_bytes = size.to_le_bytes();
        self.buffer.extend_from_slice(&size_as_bytes);
        self.buffer.extend_from_slice(payload);
    }

    pub fn get_result(mut self) -> Vec<u8> {
        let end_seq = [0u8; 4];
        self.buffer.extend_from_slice(&end_seq);
        self.buffer
    }
}
