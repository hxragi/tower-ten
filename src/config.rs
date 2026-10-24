use std::{fmt, time::Duration};

use crate::state::AdaptiveLimit;

#[derive(Debug, Clone)]
pub struct AdaptiveLimitConfig {
    pub(crate) min_limit: usize,
    pub(crate) max_limit: usize,
    pub(crate) initial_limit: usize,
    pub(crate) headroom: usize,
    pub(crate) min_rtt_window: Duration,
    pub(crate) ewma_alpha: f64,
    pub(crate) adapt_batch_size: usize,
    pub(crate) max_rampup_fraction: f64,
    pub(crate) error_decrease_factor: f64,
}

impl Default for AdaptiveLimitConfig {
    fn default() -> Self {
        Self {
            min_limit: 2,
            max_limit: 1024,
            initial_limit: 16,
            headroom: 4,
            min_rtt_window: Duration::from_secs(10 * 60),
            ewma_alpha: 0.2,
            adapt_batch_size: 32,
            max_rampup_fraction: 0.25,
            error_decrease_factor: 0.8,
        }
    }
}

impl AdaptiveLimitConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_min_limit(mut self, min_limit: usize) -> Self {
        self.min_limit = min_limit;
        self
    }

    #[must_use]
    pub fn with_max_limit(mut self, max_limit: usize) -> Self {
        self.max_limit = max_limit;
        self
    }

    #[must_use]
    pub fn with_initial_limit(mut self, initial_limit: usize) -> Self {
        self.initial_limit = initial_limit;
        self
    }

    #[must_use]
    pub fn with_headroom(mut self, headroom: usize) -> Self {
        self.headroom = headroom;
        self
    }

    #[must_use]
    pub fn with_min_rtt_window(mut self, window: Duration) -> Self {
        self.min_rtt_window = window;
        self
    }

    #[must_use]
    pub fn with_ewma_alpha(mut self, alpha: f64) -> Self {
        self.ewma_alpha = alpha;
        self
    }

    #[must_use]
    pub fn with_adapt_batch_size(mut self, batch: usize) -> Self {
        self.adapt_batch_size = batch;
        self
    }

    #[must_use]
    pub fn with_max_rampup_fraction(mut self, fraction: f64) -> Self {
        self.max_rampup_fraction = fraction;
        self
    }

    #[must_use]
    pub fn with_error_decrease_factor(mut self, factor: f64) -> Self {
        self.error_decrease_factor = factor;
        self
    }

    pub fn build(self) -> Result<AdaptiveLimit, ConfigError> {
        AdaptiveLimit::new(self)
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.min_limit == 0 {
            return Err(ConfigError::MinLimitIsZero);
        }
        if self.max_limit < self.min_limit {
            return Err(ConfigError::MaxBelowMin {
                min: self.min_limit,
                max: self.max_limit,
            });
        }
        if self.initial_limit < self.min_limit || self.initial_limit > self.max_limit {
            return Err(ConfigError::InitialLimitOutOfRange {
                initial: self.initial_limit,
                min: self.min_limit,
                max: self.max_limit,
            });
        }
        if !(self.ewma_alpha > 0.0 && self.ewma_alpha <= 1.0) {
            return Err(ConfigError::InvalidEwmaAlpha(self.ewma_alpha));
        }
        if !(self.max_rampup_fraction >= 0.0 && self.max_rampup_fraction <= 1.0) {
            return Err(ConfigError::InvalidRampupFraction(self.max_rampup_fraction));
        }
        if !(self.error_decrease_factor > 0.0 && self.error_decrease_factor < 1.0) {
            return Err(ConfigError::InvalidErrorDecreaseFactor(
                self.error_decrease_factor,
            ));
        }
        if self.adapt_batch_size == 0 {
            return Err(ConfigError::ZeroAdaptBatchSize);
        }
        if self.min_rtt_window.is_zero() {
            return Err(ConfigError::ZeroMinRttWindow);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ConfigError {
    MinLimitIsZero,
    MaxBelowMin {
        min: usize,
        max: usize,
    },
    InitialLimitOutOfRange {
        initial: usize,
        min: usize,
        max: usize,
    },
    InvalidEwmaAlpha(f64),
    InvalidRampupFraction(f64),
    InvalidErrorDecreaseFactor(f64),
    ZeroAdaptBatchSize,
    ZeroMinRttWindow,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MinLimitIsZero => f.write_str("min_limit must be >= 1"),
            Self::MaxBelowMin { min, max } => {
                write!(f, "max_limit ({max}) must be >= min_limit ({min})")
            }
            Self::InitialLimitOutOfRange { initial, min, max } => {
                write!(f, "initial_limit ({initial}) must be within [{min}, {max}]")
            }
            Self::InvalidEwmaAlpha(a) => write!(f, "ewma_alpha must be in (0, 1], got {a}"),
            Self::InvalidRampupFraction(x) => {
                write!(f, "max_rampup_fraction must be in [0, 1], got {x}")
            }
            Self::InvalidErrorDecreaseFactor(x) => {
                write!(f, "error_decrease_factor must be in (0, 1), got {x}")
            }
            Self::ZeroAdaptBatchSize => f.write_str("adapt_batch_size must be >= 1"),
            Self::ZeroMinRttWindow => f.write_str("min_rtt_window must be > 0"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        assert!(AdaptiveLimitConfig::default().build().is_ok());
        assert!(AdaptiveLimitConfig::default().validate().is_ok());
    }

    #[test]
    fn default_field_values() {
        let cfg = AdaptiveLimitConfig::default();
        assert_eq!(cfg.min_limit, 2);
        assert_eq!(cfg.max_limit, 1024);
        assert_eq!(cfg.initial_limit, 16);
        assert_eq!(cfg.headroom, 4);
        assert_eq!(cfg.min_rtt_window, Duration::from_secs(600));
        assert_eq!(cfg.ewma_alpha, 0.2);
        assert_eq!(cfg.adapt_batch_size, 32);
        assert_eq!(cfg.max_rampup_fraction, 0.25);
        assert_eq!(cfg.error_decrease_factor, 0.8);
    }

    #[test]
    fn new_is_equivalent_to_default() {
        let cfg = AdaptiveLimitConfig::new();
        assert_eq!(cfg.min_limit, AdaptiveLimitConfig::default().min_limit);
        assert_eq!(cfg.max_limit, AdaptiveLimitConfig::default().max_limit);
        assert_eq!(
            cfg.initial_limit,
            AdaptiveLimitConfig::default().initial_limit
        );
        assert_eq!(cfg.headroom, AdaptiveLimitConfig::default().headroom);
        assert_eq!(
            cfg.min_rtt_window,
            AdaptiveLimitConfig::default().min_rtt_window
        );
        assert_eq!(cfg.ewma_alpha, AdaptiveLimitConfig::default().ewma_alpha);
        assert_eq!(
            cfg.adapt_batch_size,
            AdaptiveLimitConfig::default().adapt_batch_size
        );
        assert_eq!(
            cfg.max_rampup_fraction,
            AdaptiveLimitConfig::default().max_rampup_fraction
        );
        assert_eq!(
            cfg.error_decrease_factor,
            AdaptiveLimitConfig::default().error_decrease_factor
        );
    }

    #[test]
    fn builders_override_each_field() {
        let window = Duration::from_millis(750);
        let cfg = AdaptiveLimitConfig::new()
            .with_min_limit(5)
            .with_max_limit(500)
            .with_initial_limit(50)
            .with_headroom(9)
            .with_min_rtt_window(window)
            .with_ewma_alpha(0.75)
            .with_adapt_batch_size(7)
            .with_max_rampup_fraction(0.5)
            .with_error_decrease_factor(0.25);

        assert_eq!(cfg.min_limit, 5);
        assert_eq!(cfg.max_limit, 500);
        assert_eq!(cfg.initial_limit, 50);
        assert_eq!(cfg.headroom, 9);
        assert_eq!(cfg.min_rtt_window, window);
        assert_eq!(cfg.ewma_alpha, 0.75);
        assert_eq!(cfg.adapt_batch_size, 7);
        assert_eq!(cfg.max_rampup_fraction, 0.5);
        assert_eq!(cfg.error_decrease_factor, 0.25);
    }

    #[test]
    fn builders_consume_and_return_self() {
        let base = AdaptiveLimitConfig::new().with_min_limit(3);
        let derived = base.clone().with_min_limit(4);
        assert_eq!(base.min_limit, 3);
        assert_eq!(derived.min_limit, 4);
    }

    #[test]
    fn accepts_boundary_values() {
        assert!(
            AdaptiveLimitConfig::new()
                .with_min_limit(1)
                .with_max_limit(1)
                .with_initial_limit(1)
                .validate()
                .is_ok()
        );
        assert!(
            AdaptiveLimitConfig::new()
                .with_min_limit(3)
                .with_max_limit(3)
                .with_initial_limit(3)
                .validate()
                .is_ok()
        );
        for alpha in [f64::MIN_POSITIVE, 0.5, 1.0] {
            assert!(
                AdaptiveLimitConfig::new()
                    .with_ewma_alpha(alpha)
                    .validate()
                    .is_ok()
            );
        }
        for fraction in [0.0, 0.5, 1.0] {
            assert!(
                AdaptiveLimitConfig::new()
                    .with_max_rampup_fraction(fraction)
                    .validate()
                    .is_ok()
            );
        }
        for factor in [f64::MIN_POSITIVE, 0.5, 1.0 - f64::EPSILON] {
            assert!(
                AdaptiveLimitConfig::new()
                    .with_error_decrease_factor(factor)
                    .validate()
                    .is_ok()
            );
        }
        assert!(
            AdaptiveLimitConfig::new()
                .with_adapt_batch_size(1)
                .validate()
                .is_ok()
        );
        assert!(
            AdaptiveLimitConfig::new()
                .with_min_rtt_window(Duration::from_nanos(1))
                .validate()
                .is_ok()
        );
        assert!(
            AdaptiveLimitConfig::new()
                .with_headroom(0)
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn rejects_invalid_alpha_bounds() {
        for alpha in [-f64::MIN_POSITIVE, f64::INFINITY, f64::NEG_INFINITY] {
            let err = AdaptiveLimitConfig::default()
                .with_ewma_alpha(alpha)
                .build()
                .unwrap_err();
            assert!(matches!(err, ConfigError::InvalidEwmaAlpha(_)));
        }
    }

    #[test]
    fn validation_reports_first_failing_rule() {
        assert_eq!(
            AdaptiveLimitConfig::default()
                .with_min_limit(0)
                .with_max_limit(0)
                .with_ewma_alpha(-1.0)
                .build()
                .unwrap_err(),
            ConfigError::MinLimitIsZero
        );
        assert_eq!(
            AdaptiveLimitConfig::default()
                .with_min_limit(10)
                .with_max_limit(5)
                .with_ewma_alpha(-1.0)
                .build()
                .unwrap_err(),
            ConfigError::MaxBelowMin { min: 10, max: 5 }
        );
        assert_eq!(
            AdaptiveLimitConfig::default()
                .with_initial_limit(1)
                .with_ewma_alpha(-1.0)
                .build()
                .unwrap_err(),
            ConfigError::InitialLimitOutOfRange {
                initial: 1,
                min: 2,
                max: 1024,
            }
        );
        assert_eq!(
            AdaptiveLimitConfig::default()
                .with_initial_limit(10_000)
                .with_ewma_alpha(-1.0)
                .build()
                .unwrap_err(),
            ConfigError::InitialLimitOutOfRange {
                initial: 10_000,
                min: 2,
                max: 1024,
            }
        );
        assert_eq!(
            AdaptiveLimitConfig::default()
                .with_ewma_alpha(-1.0)
                .with_max_rampup_fraction(-1.0)
                .build()
                .unwrap_err(),
            ConfigError::InvalidEwmaAlpha(-1.0)
        );
        assert_eq!(
            AdaptiveLimitConfig::default()
                .with_max_rampup_fraction(-1.0)
                .with_error_decrease_factor(-1.0)
                .build()
                .unwrap_err(),
            ConfigError::InvalidRampupFraction(-1.0)
        );
        assert_eq!(
            AdaptiveLimitConfig::default()
                .with_error_decrease_factor(-1.0)
                .with_adapt_batch_size(0)
                .build()
                .unwrap_err(),
            ConfigError::InvalidErrorDecreaseFactor(-1.0)
        );
        assert_eq!(
            AdaptiveLimitConfig::default()
                .with_adapt_batch_size(0)
                .with_min_rtt_window(Duration::ZERO)
                .build()
                .unwrap_err(),
            ConfigError::ZeroAdaptBatchSize
        );
    }

    #[test]
    fn display_messages() {
        assert_eq!(
            ConfigError::MinLimitIsZero.to_string(),
            "min_limit must be >= 1"
        );
        assert_eq!(
            ConfigError::MaxBelowMin { min: 10, max: 5 }.to_string(),
            "max_limit (5) must be >= min_limit (10)"
        );
        assert_eq!(
            ConfigError::InitialLimitOutOfRange {
                initial: 1,
                min: 2,
                max: 1024,
            }
            .to_string(),
            "initial_limit (1) must be within [2, 1024]"
        );
        assert_eq!(
            ConfigError::InvalidEwmaAlpha(0.5).to_string(),
            "ewma_alpha must be in (0, 1], got 0.5"
        );
        assert_eq!(
            ConfigError::InvalidRampupFraction(1.5).to_string(),
            "max_rampup_fraction must be in [0, 1], got 1.5"
        );
        assert_eq!(
            ConfigError::InvalidErrorDecreaseFactor(1.5).to_string(),
            "error_decrease_factor must be in (0, 1), got 1.5"
        );
        assert_eq!(
            ConfigError::ZeroAdaptBatchSize.to_string(),
            "adapt_batch_size must be >= 1"
        );
        assert_eq!(
            ConfigError::ZeroMinRttWindow.to_string(),
            "min_rtt_window must be > 0"
        );
    }

    #[test]
    fn config_error_derives_debug_clone_and_partial_eq() {
        let err = AdaptiveLimitConfig::default()
            .with_max_limit(5)
            .with_min_limit(10)
            .build()
            .unwrap_err();

        assert_eq!(err.clone(), err);
        assert_ne!(err, ConfigError::MinLimitIsZero);
        assert!(format!("{err:?}").contains("MaxBelowMin"));
    }

    #[test]
    fn config_error_is_std_error_without_source() {
        use std::error::Error as _;

        let err = AdaptiveLimitConfig::default()
            .with_min_limit(0)
            .build()
            .unwrap_err();
        assert!(err.source().is_none());
        assert!(format!("{err:?}").contains("MinLimitIsZero"));

        let boxed: Box<dyn std::error::Error> = Box::new(err);
        assert_eq!(boxed.to_string(), "min_limit must be >= 1");
    }

    #[test]
    fn rejects_zero_batch_and_window() {
        let err = AdaptiveLimitConfig::default()
            .with_adapt_batch_size(0)
            .build()
            .unwrap_err();
        assert_eq!(err, ConfigError::ZeroAdaptBatchSize);

        let err = AdaptiveLimitConfig::default()
            .with_min_rtt_window(Duration::ZERO)
            .build()
            .unwrap_err();
        assert_eq!(err, ConfigError::ZeroMinRttWindow);
    }

    #[test]
    fn rejects_zero_min_limit() {
        let err = AdaptiveLimitConfig::default()
            .with_min_limit(0)
            .build()
            .unwrap_err();
        assert_eq!(err, ConfigError::MinLimitIsZero);
    }

    #[test]
    fn rejects_max_below_min() {
        let err = AdaptiveLimitConfig::default()
            .with_min_limit(10)
            .with_max_limit(5)
            .build()
            .unwrap_err();
        assert_eq!(err, ConfigError::MaxBelowMin { min: 10, max: 5 });
    }

    #[test]
    fn rejects_initial_out_of_range() {
        let err = AdaptiveLimitConfig::default()
            .with_initial_limit(1)
            .build()
            .unwrap_err();
        assert!(matches!(err, ConfigError::InitialLimitOutOfRange { .. }));
        let err = AdaptiveLimitConfig::default()
            .with_initial_limit(10_000)
            .build()
            .unwrap_err();
        assert!(matches!(err, ConfigError::InitialLimitOutOfRange { .. }));
    }

    #[test]
    fn rejects_invalid_alpha() {
        for alpha in [0.0, -0.5, 1.5, f64::NAN] {
            let err = AdaptiveLimitConfig::default()
                .with_ewma_alpha(alpha)
                .build()
                .unwrap_err();
            assert!(matches!(err, ConfigError::InvalidEwmaAlpha(_)));
        }
    }

    #[test]
    fn rejects_invalid_rampup() {
        for x in [-0.1, 1.5, f64::NAN] {
            let err = AdaptiveLimitConfig::default()
                .with_max_rampup_fraction(x)
                .build()
                .unwrap_err();
            assert!(matches!(err, ConfigError::InvalidRampupFraction(_)));
        }
    }

    #[test]
    fn rejects_invalid_error_decrease() {
        for x in [0.0, 1.0, 1.5, f64::NAN] {
            let err = AdaptiveLimitConfig::default()
                .with_error_decrease_factor(x)
                .build()
                .unwrap_err();
            assert!(matches!(err, ConfigError::InvalidErrorDecreaseFactor(_)));
        }
    }
}
