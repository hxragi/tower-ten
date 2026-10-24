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
#[non_exhaustive]
pub struct AdaptiveLimitSnapshot {
    pub current_limit: usize,
    pub active_requests: usize,
    pub min_rtt: Option<Duration>,
    pub ewma_rtt: Option<Duration>,
    pub total_completed: u64,
    pub total_errors: u64,
    pub total_dropped: u64,
    pub total_rejected: u64,
    pub total_adaptations: u64,
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
                Ok(_) => break,
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

        self.completed.swap(0, Ordering::Relaxed);
        let dropped = self.dropped.swap(0, Ordering::Relaxed);
        let errors = self.errors.swap(0, Ordering::Relaxed);
        let epoch_rtt_min = self.epoch_rtt_min.swap(u64::MAX, Ordering::Relaxed);

        self.adapt(dropped, errors, epoch_rtt_min);

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
                ((prev_ewma as f64 * (1.0 - alpha) + epoch_rtt_min as f64 * alpha) as u64).max(1)
            };
            self.rtt_ewma.store(ewma, Ordering::Relaxed);

            let rtt_min = self.rtt_min.load(Ordering::Relaxed);
            let gradient = (rtt_min as f64 / ewma as f64).clamp(0.0, 1.0);
            new_limit = limit * gradient + self.cfg.headroom as f64;
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
            total_adaptations: self.total_adaptations.load(Ordering::Relaxed),
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

    fn batch_cfg(batch: usize) -> AdaptiveLimitConfig {
        AdaptiveLimitConfig::new()
            .with_min_limit(1)
            .with_max_limit(1000)
            .with_initial_limit(10)
            .with_headroom(4)
            .with_adapt_batch_size(batch)
            .with_ewma_alpha(0.2)
            .with_max_rampup_fraction(0.5)
            .with_error_decrease_factor(0.8)
    }

    fn complete_batch(limiter: &AdaptiveLimit, n: usize, rtt: Duration, success: bool) {
        for _ in 0..n {
            let permit = limiter
                .try_acquire_permit()
                .expect("slot must be available");
            permit.complete_with(rtt, success);
        }
    }

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
    }

    #[test]
    fn shared_new_converts_window_to_nanoseconds() {
        let shared =
            Shared::new(AdaptiveLimitConfig::new().with_min_rtt_window(Duration::from_secs(2)));
        assert_eq!(shared.min_rtt_window_ns, 2_000_000_000);
    }

    #[test]
    fn shared_new_clamps_oversized_window_to_u64_max() {
        let shared = Shared::new(AdaptiveLimitConfig::new().with_min_rtt_window(Duration::MAX));
        assert_eq!(shared.min_rtt_window_ns, u64::MAX);
    }

    #[test]
    fn shared_accessors() {
        let limiter = AdaptiveLimit::new(batch_cfg(1_000_000).with_initial_limit(7)).unwrap();

        assert_eq!(limiter.current_limit(), 7);
        assert_eq!(limiter.active_requests(), 0);

        let permit = limiter.try_acquire_permit().unwrap();
        assert_eq!(limiter.active_requests(), 1);
        assert_eq!(limiter.snapshot().current_limit, 7);
        assert_eq!(limiter.snapshot().active_requests, 1);

        drop(permit);
        assert_eq!(limiter.active_requests(), 0);
    }

    #[test]
    fn try_acquire_and_rejection_at_limit() {
        let limiter = AdaptiveLimit::new(
            AdaptiveLimitConfig::new()
                .with_min_limit(2)
                .with_max_limit(2)
                .with_initial_limit(2)
                .with_adapt_batch_size(1_000_000),
        )
        .unwrap();

        let p1 = limiter.try_acquire_permit().unwrap();
        let p2 = limiter.try_acquire_permit().unwrap();
        assert!(limiter.try_acquire_permit().is_none());

        assert_eq!(limiter.active_requests(), 2);
        assert_eq!(limiter.snapshot().total_rejected, 1);

        drop(p1);
        assert!(limiter.try_acquire_permit().is_some());
        drop(p2);
    }

    #[test]
    fn concurrent_acquire_complete_survives_contention() {
        let limiter = Arc::new(
            AdaptiveLimit::new(
                AdaptiveLimitConfig::new()
                    .with_min_limit(64)
                    .with_max_limit(64)
                    .with_initial_limit(64)
                    .with_adapt_batch_size(usize::MAX),
            )
            .unwrap(),
        );

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let limiter = Arc::clone(&limiter);
                std::thread::spawn(move || {
                    for i in 0..5_000 {
                        let permit = limiter.try_acquire_permit().expect("slot must be available");
                        let ms = ((i + t) % 9) as u64 + 1;
                        permit.complete_with(Duration::from_millis(ms), true);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let snap = limiter.snapshot();
        assert_eq!(snap.active_requests, 0);
        assert_eq!(snap.total_completed, 40_000);
        assert_eq!(snap.total_dropped, 0);
        assert_eq!(snap.total_rejected, 0);
        assert_eq!(snap.total_adaptations, 0);
        assert_eq!(snap.min_rtt, Some(Duration::from_millis(1)));
    }

    #[test]
    fn rtt_cas_loop_smaller_observation() {
        let limiter = AdaptiveLimit::new(batch_cfg(1_000_000)).unwrap();

        complete_batch(&limiter, 1, Duration::from_millis(10), true);
        complete_batch(&limiter, 1, Duration::from_millis(5), true);
        complete_batch(&limiter, 1, Duration::from_millis(20), true);

        assert_eq!(limiter.snapshot().min_rtt, Some(Duration::from_millis(5)));
    }

    #[test]
    fn rtt_cas_contention_triggers_retry() {
        use std::sync::atomic::AtomicBool;

        let shared = Arc::new(Shared::new(batch_cfg(1_000_000)));
        shared.window_start_ns.store(u64::MAX, Relaxed);

        let stop = Arc::new(AtomicBool::new(false));
        let s_reset = Arc::clone(&shared);
        let stop_reset = Arc::clone(&stop);

        let resetter = std::thread::spawn(move || {
            while !stop_reset.load(Relaxed) {
                s_reset.rtt_min.store(1_000, Relaxed);
                std::hint::spin_loop();
            }
        });

        let workers: Vec<_> = (0..8)
            .map(|_| {
                let s = Arc::clone(&shared);
                std::thread::spawn(move || {
                    for _ in 0..10_000 {
                        s.observe_rtt(500);
                        s.observe_rtt(200);
                        s.observe_rtt(100);
                    }
                })
            })
            .collect();

        for w in workers {
            w.join().unwrap();
        }
        stop.store(true, Relaxed);
        resetter.join().unwrap();
    }

    #[test]
    fn complete_and_rtt_window_advancement() {
        let limiter =
            AdaptiveLimit::new(batch_cfg(1_000_000).with_min_rtt_window(Duration::from_millis(20)))
                .unwrap();

        complete_batch(&limiter, 1, Duration::from_millis(10), true);
        assert_eq!(limiter.snapshot().min_rtt, Some(Duration::from_millis(10)));

        std::thread::sleep(Duration::from_millis(30));

        complete_batch(&limiter, 1, Duration::from_millis(30), true);
        assert_eq!(limiter.snapshot().min_rtt, Some(Duration::from_millis(30)));
    }

    #[test]
    fn complete_clamps_zero_rtt_to_one_ns() {
        let limiter = AdaptiveLimit::new(batch_cfg(1_000_000)).unwrap();

        complete_batch(&limiter, 1, Duration::ZERO, true);

        assert_eq!(limiter.snapshot().min_rtt, Some(Duration::from_nanos(1)));
    }

    #[test]
    fn adapt_rampup_increases_limit() {
        let limiter = AdaptiveLimit::new(batch_cfg(10)).unwrap();

        complete_batch(&limiter, 10, Duration::from_millis(10), true);
        assert_eq!(limiter.current_limit(), 14);

        complete_batch(&limiter, 10, Duration::from_millis(10), true);
        assert_eq!(limiter.current_limit(), 18);
    }

    #[test]
    fn adapt_shrinks_limit_when_rtt_grows() {
        let limiter = AdaptiveLimit::new(batch_cfg(10).with_initial_limit(16)).unwrap();

        complete_batch(&limiter, 10, Duration::from_millis(10), true);
        assert_eq!(limiter.current_limit(), 20);

        complete_batch(&limiter, 10, Duration::from_millis(30), true);
        assert_eq!(limiter.current_limit(), 18);
    }

    #[test]
    fn adapt_uses_batch_min_rtt_for_ewma() {
        let limiter = AdaptiveLimit::new(batch_cfg(10).with_initial_limit(16)).unwrap();

        complete_batch(&limiter, 5, Duration::from_millis(10), true);
        complete_batch(&limiter, 5, Duration::from_millis(100), true);

        assert_eq!(limiter.current_limit(), 20);
        assert_eq!(limiter.snapshot().ewma_rtt, Some(Duration::from_millis(10)));
    }

    #[test]
    fn adapt_on_errors_decreases_limit() {
        let limiter =
            AdaptiveLimit::new(batch_cfg(10).with_initial_limit(100).with_headroom(0)).unwrap();

        complete_batch(&limiter, 10, Duration::from_millis(10), false);
        assert_eq!(limiter.current_limit(), 80);

        complete_batch(&limiter, 10, Duration::from_millis(10), false);
        assert_eq!(limiter.current_limit(), 64);
    }

    #[test]
    fn adapt_error_decrease_beats_gradient_growth() {
        let limiter = AdaptiveLimit::new(batch_cfg(10).with_initial_limit(50)).unwrap();

        complete_batch(&limiter, 10, Duration::from_millis(10), false);

        assert_eq!(limiter.current_limit(), 40);
    }

    #[test]
    fn adapt_respects_min_clamp() {
        let limiter = AdaptiveLimit::new(
            batch_cfg(10)
                .with_min_limit(5)
                .with_initial_limit(20)
                .with_headroom(0),
        )
        .unwrap();

        complete_batch(&limiter, 80, Duration::from_millis(10), false);

        assert_eq!(limiter.current_limit(), 5);
    }

    #[test]
    fn adapt_respects_max_clamp() {
        let limiter = AdaptiveLimit::new(
            AdaptiveLimitConfig::new()
                .with_min_limit(1)
                .with_max_limit(5)
                .with_initial_limit(2)
                .with_headroom(100)
                .with_adapt_batch_size(10)
                .with_ewma_alpha(0.2)
                .with_max_rampup_fraction(0.5),
        )
        .unwrap();

        complete_batch(&limiter, 10, Duration::from_millis(10), true);
        assert_eq!(limiter.current_limit(), 3);

        complete_batch(&limiter, 10, Duration::from_millis(10), true);
        assert_eq!(limiter.current_limit(), 5);

        complete_batch(&limiter, 10, Duration::from_millis(10), true);
        assert_eq!(limiter.current_limit(), 5);
    }

    #[test]
    fn adapt_on_dropped_triggers_adaptation() {
        let limiter = AdaptiveLimit::new(batch_cfg(10)).unwrap();

        complete_batch(&limiter, 10, Duration::from_millis(10), true);
        assert_eq!(limiter.current_limit(), 14);

        complete_batch(&limiter, 5, Duration::from_millis(10), true);
        for _ in 0..10 {
            drop(limiter.try_acquire_permit().unwrap());
        }

        assert_eq!(limiter.current_limit(), 14);

        let snap = limiter.snapshot();
        assert_eq!(snap.total_dropped, 10);
        assert_eq!(snap.active_requests, 0);
        assert_eq!(snap.total_completed, 15);
    }

    #[test]
    fn adapt_without_rtt_samples_keeps_limit() {
        let limiter =
            AdaptiveLimit::new(batch_cfg(10).with_initial_limit(16).with_headroom(4)).unwrap();

        for _ in 0..10 {
            drop(limiter.try_acquire_permit().unwrap());
        }

        assert_eq!(limiter.current_limit(), 16);

        let snap = limiter.snapshot();
        assert_eq!(snap.total_adaptations, 1);
        assert_eq!(snap.ewma_rtt, None);
        assert_eq!(snap.min_rtt, None);
    }

    #[test]
    fn cancelled_requests_do_not_pollute_rtt_stats() {
        let limiter =
            AdaptiveLimit::new(AdaptiveLimitConfig::new().with_adapt_batch_size(1_000_000))
                .unwrap();

        for _ in 0..3 {
            drop(limiter.try_acquire_permit().unwrap());
        }

        let snap = limiter.snapshot();
        assert_eq!(snap.active_requests, 0);
        assert_eq!(snap.total_dropped, 3);
        assert_eq!(snap.total_completed, 0);
        assert_eq!(snap.min_rtt, None);
        assert_eq!(snap.ewma_rtt, None);
    }

    #[test]
    fn maybe_adapt_skips_when_already_adapting() {
        let shared = Shared::new(batch_cfg(4).with_initial_limit(10).with_headroom(0));

        shared.adapting.store(1, Relaxed);

        for _ in 0..4 {
            shared.complete(1_000_000, true);
        }

        assert_eq!(shared.limit.load(Relaxed), 10);
        assert_eq!(shared.completed.load(Relaxed), 4);
        assert_eq!(shared.adapting.load(Relaxed), 1);

        shared.adapting.store(0, Relaxed);
    }

    #[test]
    fn maybe_adapt_ignores_below_batch() {
        let shared = Shared::new(batch_cfg(8));

        shared.complete(1_000, true);
        shared.maybe_adapt();

        assert_eq!(shared.limit.load(Relaxed), 10);
        assert_eq!(shared.completed.load(Relaxed), 1);
        assert_eq!(shared.epoch_rtt_min.load(Relaxed), 1_000);
        assert_eq!(shared.adapting.load(Relaxed), 0);
    }

    #[test]
    fn ewma_smoothing_matches_formula() {
        let limiter =
            AdaptiveLimit::new(batch_cfg(1).with_initial_limit(16).with_headroom(0)).unwrap();

        complete_batch(&limiter, 1, Duration::from_millis(100), true);
        complete_batch(&limiter, 1, Duration::from_millis(200), true);

        let ewma = limiter.snapshot().ewma_rtt.unwrap().as_nanos();
        assert!((ewma as i128 - 120_000_000i128).abs() <= 2);
        assert_eq!(limiter.current_limit(), 13);
    }

    #[test]
    fn snapshot_derives_debug_copy_partial_eq_and_eq() {
        let limiter = AdaptiveLimit::new(AdaptiveLimitConfig::new()).unwrap();
        let snap = limiter.snapshot();
        let copy = snap;
        assert_eq!(snap, copy);
        assert_ne!(
            snap,
            AdaptiveLimitSnapshot {
                current_limit: snap.current_limit + 1,
                ..snap
            }
        );
        assert!(format!("{copy:?}").contains("AdaptiveLimitSnapshot"));
    }
}
