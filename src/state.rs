use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, AtomicUsize},
    },
    time::{Duration, Instant},
};

use crate::config::{AdaptiveLimitConfig, ConfigError};

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

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
}
