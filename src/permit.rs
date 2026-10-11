use std::{
    cell::Cell,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::state::Shared;

pub struct Permit {
    shared: Arc<Shared>,
    started_at: Instant,
    completed: Cell<bool>,
}

impl Permit {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            started_at: Instant::now(),
            completed: Cell::new(false),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn complete(&mut self, success: bool) {
        let rtt = self.started_at.elapsed();
        self.complete_with(rtt, success)
    }

    pub(crate) fn complete_with(&self, rtt: Duration, success: bool) {
        if self.completed.get() {
            return;
        }
        self.completed.set(true);

        let rtt_ns = rtt.as_nanos().min(u64::MAX as u128) as u64;
        self.shared.complete(rtt_ns, success);
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if !self.completed.get() {
            self.shared.cancel();
        }
    }
}

impl fmt::Debug for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Permit")
            .field("started_at", &self.started_at)
            .field("completed", &self.completed.get())
            .finish()
    }
}
