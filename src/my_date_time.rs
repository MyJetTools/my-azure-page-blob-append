use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug)]
pub struct DateTimeAsMicroseconds {
    pub unix_microseconds: i64,
}

impl DateTimeAsMicroseconds {
    pub fn now() -> Self {
        let unix_microseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        Self { unix_microseconds }
    }
}
