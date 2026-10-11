use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use crate::{
    config::{AdaptiveLimitConfig, ConfigError},
    permit::Permit,
};

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

#[inline]
pub(crate) fn mono_now_ns() -> u64 {
    let elapsed = PROCESS_START.get_or_init(Instant::now).elapsed();
    elapsed.as_nanos() as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveLimitSnapshot {
    pub current_limit: usize,
    pub active_requests: usize,
    pub min_rtt: Option<Duration>,
    pub ewma_rtt: Option<Duration>,
    pub total_completed: u64,
    pub total_errors: u64,
    pub total_dropped: u64,
    pub total_rejected: u64,
    pub total_adptations: u64,
}

#[derive(Debug, Clone)]
pub struct AdaptiveLimit {
    shared: Arc<Shared>,
}

impl AdaptiveLimit {
    pub fn new(config: AdaptiveLimitConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            shared: Arc::new(Shared::new(config)),
        })
    }

    pub fn try_acquire_permit(&self) -> Option<Permit> {
        if self.shared.try_acquire() {
            Some(Permit::new(Arc::clone(&self.shared)))
        } else {
            None
        }
    }

    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    pub fn current_limit(&self) -> usize {
        self.shared.limit.load(Ordering::Relaxed)
    }

    pub fn active_requests(&self) -> usize {
        self.shared.active.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> AdaptiveLimitSnapshot {
        self.shared.snapshot()
    }
}

#[derive(Debug)]
pub(crate) struct Shared {
    cfg: AdaptiveLimitConfig,
    min_rtt_window_ns: u64,
    active: AtomicUsize,
    limit: AtomicUsize,
    rtt_min: AtomicU64,
    window_start_ns: AtomicU64,
    rtt_ewma: AtomicU64,
    completed: AtomicUsize,
    errors: AtomicUsize,
    dropped: AtomicUsize,
    epoch_rtt_min: AtomicU64,
    adapting: AtomicUsize,
    total_completed: AtomicU64,
    total_errors: AtomicU64,
    total_dropped: AtomicU64,
    total_rejected: AtomicU64,
    total_adaptations: AtomicU64,
}

impl Shared {
    pub(crate) fn new(cfg: AdaptiveLimitConfig) -> Self {
        let initial_limit = cfg.initial_limit;
        let min_rtt_window_ns = cfg.min_rtt_window.as_nanos().min(u64::MAX as u128) as u64;
        Self {
            cfg,
            min_rtt_window_ns,
            active: AtomicUsize::new(0),
            limit: AtomicUsize::new(initial_limit),
            rtt_min: AtomicU64::new(0),
            window_start_ns: AtomicU64::new(0),
            rtt_ewma: AtomicU64::new(0),
            completed: AtomicUsize::new(0),
            errors: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            epoch_rtt_min: AtomicU64::new(u64::MAX),
            adapting: AtomicUsize::new(0),
            total_completed: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            total_dropped: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_adaptations: AtomicU64::new(0),
        }
    }

    #[inline]
    pub(crate) fn try_acquire(&self) -> bool {
        loop {
            let current = self.active.load(Ordering::Acquire);
            if current >= self.limit.load(Ordering::Acquire) {
                self.total_rejected.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            if self
                .active
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    pub(crate) fn complete(&self, rtt_ns: u64, success: bool) {
        self.active.fetch_sub(1, Ordering::Release);
        let rtt_ns = rtt_ns.max(1);
        self.observe_rtt(rtt_ns);
        self.epoch_rtt_min.fetch_min(rtt_ns, Ordering::Relaxed);

        self.total_completed.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.errors.fetch_add(1, Ordering::Relaxed);
            self.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        let completed = self.completed.fetch_add(1, Ordering::AcqRel) + 1;
        if completed >= self.cfg.adapt_batch_size {
            self.maybe_adapt();
        }
    }

    pub(crate) fn cancel(&self) {
        self.active.fetch_sub(1, Ordering::Release);
        self.total_dropped.fetch_add(1, Ordering::Relaxed);

        let dropped = self.dropped.fetch_add(1, Ordering::AcqRel) + 1;
        if dropped >= self.cfg.adapt_batch_size {
            self.maybe_adapt();
        }
    }

    #[inline]
    fn observe_rtt(&self, rtt_ns: u64) {
        let now = mono_now_ns();
        let window_start = self.window_start_ns.load(Ordering::Relaxed);

        if now.saturating_sub(window_start) >= self.min_rtt_window_ns
            || self.rtt_min.load(Ordering::Relaxed) == 0
        {
            self.rtt_min.store(rtt_ns, Ordering::Relaxed);
            self.window_start_ns.store(now, Ordering::Relaxed);
            return;
        }

        let mut current = self.rtt_min.load(Ordering::Relaxed);
        while rtt_ns < current {
            match self.rtt_min.compare_exchange_weak(
                current,
                rtt_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(()) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn maybe_adapt(&self) {
        if self.completed.load(Ordering::Relaxed) + self.dropped.load(Ordering::Relaxed)
            < self.cfg.adapt_batch_size
        {
            return;
        }

        if self
            .adapting
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let completed = self.completed.swap(0, Ordering::Relaxed);
        let dropped = self.dropped.swap(0, Ordering::Relaxed);
        let errors = self.errors.swap(0, Ordering::Relaxed);
        let epoch_rtt_min = self.epoch_rtt_min.swap(u64::MAX, Ordering::Relaxed);

        if completed + dropped >= self.cfg.adapt_batch_size {
            self.adapt(dropped, errors, epoch_rtt_min);
        } else {
            self.completed.fetch_add(completed, Ordering::Relaxed);
            self.dropped.fetch_add(dropped, Ordering::Relaxed);
            self.errors.fetch_add(errors, Ordering::Relaxed);
            if epoch_rtt_min != u64::MAX {
                self.epoch_rtt_min
                    .fetch_min(epoch_rtt_min, Ordering::Relaxed);
            }
        }

        self.adapting.store(0, Ordering::Release);
    }

    fn adapt(&self, dropped: usize, errors: usize, epoch_rtt_min: u64) {
        let limit = self.limit.load(Ordering::Relaxed) as f64;
        let mut new_limit = limit;

        if epoch_rtt_min != u64::MAX {
            let prev_ewma = self.rtt_ewma.load(Ordering::Relaxed);
            let ewma = if prev_ewma == 0 {
                epoch_rtt_min
            } else {
                let alpha = self.cfg.ewma_alpha;
                (prev_ewma as f64 * (1.0 - alpha) + epoch_rtt_min as f64 * alpha) as u64
            };
            self.rtt_ewma.store(ewma, Ordering::Relaxed);

            let rtt_min = self.rtt_min.load(Ordering::Relaxed);
            if ewma > 0 {
                let gradient = (rtt_min as f64 / ewma as f64).clamp(0.0, 1.0);
                new_limit = limit * gradient + self.cfg.headroom as f64;
            }
        }

        if errors > 0 {
            let decreased = limit * self.cfg.error_decrease_factor;
            new_limit = new_limit.min(decreased);
        }

        if dropped > 0 {
            new_limit = new_limit.min(limit);
        }

        if new_limit > limit {
            let capped = (limit * (1.0 + self.cfg.max_rampup_fraction)).max(limit + 1.0);
            new_limit = new_limit.min(capped);
        }

        let new_limit = (new_limit.round() as usize).clamp(self.cfg.min_limit, self.cfg.max_limit);
        self.limit.store(new_limit, Ordering::Relaxed);
        self.total_adaptations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> AdaptiveLimitSnapshot {
        AdaptiveLimitSnapshot {
            current_limit: self.limit.load(Ordering::Relaxed),
            active_requests: self.active.load(Ordering::Relaxed),
            min_rtt: nz_duration(self.rtt_min.load(Ordering::Relaxed)),
            ewma_rtt: nz_duration(self.rtt_ewma.load(Ordering::Relaxed)),
            total_completed: self.total_completed.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            total_dropped: self.total_dropped.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
            total_adptations: self.total_adaptations.load(Ordering::Relaxed),
        }
    }
}

fn nz_duration(ns: u64) -> Option<Duration> {
    if ns == 0 {
        None
    } else {
        Some(Duration::from_nanos(ns))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering::Relaxed;

    use super::*;

    #[test]
    fn mono_now_ns_initializes_process_start_and_is_monotonic() {
        let first = mono_now_ns();
        assert!(
            PROCESS_START.get().is_some(),
            "PROCESS_START must be initialized by mono_now_ns"
        );

        let mut previous = first;
        for _ in 0..1000 {
            let now = mono_now_ns();
            assert!(now >= previous, "clock went backwards: {now} < {previous}");
            previous = now;
        }
        assert!(
            previous - first < 60_000_000_000,
            "1000 iterations should take far less than a minute"
        );
    }

    #[test]
    fn mono_now_ns_is_shared_across_threads() {
        let samples: Vec<u64> =
            std::thread::spawn(|| (0..8).map(|_| mono_now_ns()).collect::<Vec<_>>())
                .join()
                .expect("sampling thread panicked");

        assert_eq!(samples.len(), 8);
        let min = samples.iter().copied().min().expect("no samples");
        let max = samples.iter().copied().max().expect("no samples");
        assert!(min <= max);
        assert!(
            max - min < 60_000_000_000,
            "threads must observe the same monotonic origin"
        );
    }

    #[test]
    fn adaptive_limit_new_starts_at_configured_limit() {
        for initial in [2, 16, 1024] {
            let limit = AdaptiveLimit::new(
                AdaptiveLimitConfig::new()
                    .with_min_limit(1)
                    .with_max_limit(2048)
                    .with_initial_limit(initial),
            )
            .expect("valid config must build");

            assert_eq!(limit.shared.limit.load(Relaxed), initial);
            assert_eq!(limit.shared.active.load(Relaxed), 0);
        }
    }

    #[test]
    fn adaptive_limit_new_rejects_invalid_config() {
        assert_eq!(
            AdaptiveLimit::new(AdaptiveLimitConfig::new().with_min_limit(0)).unwrap_err(),
            ConfigError::MinLimitIsZero
        );
    }

    #[test]
    fn adaptive_limit_clone_shares_state() {
        let limit =
            AdaptiveLimit::new(AdaptiveLimitConfig::new()).expect("default config is valid");
        let clone = limit.clone();

        limit.shared.limit.store(64, Relaxed);
        assert_eq!(clone.shared.limit.load(Relaxed), 64);
        assert!(Arc::ptr_eq(&limit.shared, &clone.shared));
        assert!(format!("{limit:?}").contains("AdaptiveLimit"));
    }

    #[test]
    fn shared_new_initializes_all_counters_to_zero() {
        let shared = Shared::new(AdaptiveLimitConfig::new());

        assert_eq!(shared.limit.load(Relaxed), 16);
        assert_eq!(shared.active.load(Relaxed), 0);
        assert_eq!(shared.completed.load(Relaxed), 0);
        assert_eq!(shared.errors.load(Relaxed), 0);
        assert_eq!(shared.dropped.load(Relaxed), 0);
        assert_eq!(shared.adapting.load(Relaxed), 0);

        assert_eq!(shared.rtt_min.load(Relaxed), 0);
        assert_eq!(shared.rtt_ewma.load(Relaxed), 0);
        assert_eq!(shared.window_start_ns.load(Relaxed), 0);
        assert_eq!(shared.epoch_rtt_min.load(Relaxed), u64::MAX);

        assert_eq!(shared.total_completed.load(Relaxed), 0);
        assert_eq!(shared.total_errors.load(Relaxed), 0);
        assert_eq!(shared.total_dropped.load(Relaxed), 0);
        assert_eq!(shared.total_rejected.load(Relaxed), 0);
        assert_eq!(shared.total_adaptations.load(Relaxed), 0);

        assert_eq!(shared.cfg.initial_limit, 16);
        assert!(format!("{shared:?}").contains("Shared"));
    }

    #[test]
    fn shared_new_converts_window_to_nanoseconds() {
        let shared =
            Shared::new(AdaptiveLimitConfig::new().with_min_rtt_window(Duration::from_secs(3)));
        assert_eq!(shared.min_rtt_window_ns, 3_000_000_000);

        let tiny =
            Shared::new(AdaptiveLimitConfig::new().with_min_rtt_window(Duration::from_nanos(1)));
        assert_eq!(tiny.min_rtt_window_ns, 1);
    }

    #[test]
    fn shared_new_clamps_oversized_window_to_u64_max() {
        let shared = Shared::new(AdaptiveLimitConfig::new().with_min_rtt_window(Duration::MAX));
        assert!(Duration::MAX.as_nanos() > u64::MAX as u128);
        assert_eq!(shared.min_rtt_window_ns, u64::MAX);
    }

    #[test]
    fn snapshot_derives_debug_copy_partial_eq_and_eq() {
        let snapshot = AdaptiveLimitSnapshot {
            current_limit: 16,
            active_requests: 3,
            min_rtt: Some(Duration::from_millis(5)),
            ewma_rtt: Some(Duration::from_millis(7)),
            total_completed: 10,
            total_errors: 1,
            total_dropped: 2,
            total_rejected: 4,
            total_adptations: 5,
        };

        assert_eq!(snapshot, snapshot);
        assert_eq!(Clone::clone(&snapshot), snapshot);
        assert!(format!("{snapshot:?}").contains("current_limit: 16"));

        let mut other = snapshot;
        other.current_limit = 17;
        assert_ne!(other, snapshot);
        assert_eq!(snapshot.current_limit, 16, "snapshot must be Copy");

        let idle = AdaptiveLimitSnapshot {
            current_limit: 1,
            active_requests: 0,
            min_rtt: None,
            ewma_rtt: None,
            total_completed: 0,
            total_errors: 0,
            total_dropped: 0,
            total_rejected: 0,
            total_adptations: 0,
        };
        assert_ne!(idle, snapshot);
    }
}
