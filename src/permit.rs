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
        self.complete_with(rtt, success);
    }

    pub(crate) fn complete_with(&self, rtt: Duration, success: bool) {
        if self.completed.get() {
            return;
        }
        self.completed.set(true);

        let rtt_ns = rtt.as_nanos().min((u64::MAX - 1) as u128) as u64;
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit")
            .field("started_at", &self.started_at)
            .field("completed", &self.completed.get())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AdaptiveLimitConfig;

    fn shared() -> Arc<Shared> {
        Arc::new(Shared::new(AdaptiveLimitConfig::new()))
    }

    fn permit(shared: &Arc<Shared>) -> Permit {
        assert!(shared.try_acquire());
        Permit::new(Arc::clone(shared))
    }

    #[test]
    fn permit_elapsed_increases() {
        let shared = shared();
        let permit = permit(&shared);
        std::thread::sleep(Duration::from_millis(5));
        assert!(permit.elapsed() >= Duration::from_millis(4));
    }

    #[test]
    fn permit_complete_records_success() {
        let shared = shared();
        let mut permit = permit(&shared);
        permit.complete(true);

        let snap = shared.snapshot();
        assert_eq!(snap.total_completed, 1);
        assert_eq!(snap.total_errors, 0);
        assert_eq!(snap.total_dropped, 0);
        assert_eq!(snap.active_requests, 0);

        permit.complete(false);
        assert_eq!(shared.snapshot().total_completed, 1);
    }

    #[test]
    fn permit_complete_records_failure() {
        let shared = shared();
        let mut permit = permit(&shared);
        permit.complete(false);

        let snap = shared.snapshot();
        assert_eq!(snap.total_completed, 1);
        assert_eq!(snap.total_errors, 1);
        assert_eq!(snap.active_requests, 0);
    }

    #[test]
    fn permit_dropped_without_completion_cancels() {
        let shared = shared();
        let permit = permit(&shared);
        drop(permit);

        let snap = shared.snapshot();
        assert_eq!(snap.total_dropped, 1);
        assert_eq!(snap.total_completed, 0);
        assert_eq!(snap.active_requests, 0);
    }

    #[test]
    fn permit_drop_after_completion_does_not_cancel() {
        let shared = shared();
        let mut permit = permit(&shared);
        permit.complete(true);
        drop(permit);

        let snap = shared.snapshot();
        assert_eq!(snap.total_dropped, 0);
        assert_eq!(snap.total_completed, 1);
        assert_eq!(snap.active_requests, 0);
    }

    #[test]
    fn permit_complete_with_overflowing_duration_clamps() {
        let shared = shared();
        let permit = permit(&shared);
        permit.complete_with(Duration::MAX, true);

        let snap = shared.snapshot();
        assert_eq!(snap.total_completed, 1);
        assert_eq!(snap.min_rtt, Some(Duration::from_nanos(u64::MAX - 1)));
        assert_eq!(snap.active_requests, 0);
    }

    #[test]
    fn permit_debug_format() {
        let shared = shared();
        let permit = permit(&shared);
        let debug_str = format!("{permit:?}");
        assert!(debug_str.contains("Permit"));
        assert!(debug_str.contains("completed: false"));
    }
}
