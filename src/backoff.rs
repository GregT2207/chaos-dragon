use std::{cmp, time::Duration};

use time::OffsetDateTime;

const MULTIPLIER: u32 = 2;
const BACKOFF_LIMIT_SECONDS: u64 = 256;

pub struct ExponentialBackoff {
    current_backoff: Duration,
    next_retry_at: OffsetDateTime,
}

impl ExponentialBackoff {
    pub fn new() -> Self {
        ExponentialBackoff {
            current_backoff: Duration::from_secs(2),
            next_retry_at: OffsetDateTime::now_utc(),
        }
    }

    pub fn remaining_wait(&mut self) -> Duration {
        let wait = self.next_retry_at - OffsetDateTime::now_utc();

        // Implicitly increase the value for next time when requested
        self.next_retry_at += self.current_backoff;
        self.current_backoff = cmp::min(
            self.current_backoff * MULTIPLIER,
            Duration::from_secs(BACKOFF_LIMIT_SECONDS),
        );

        wait.try_into().unwrap_or(Duration::from_secs(0))
    }

    pub fn reset(&mut self) {
        self.next_retry_at = OffsetDateTime::now_utc();
    }
}
