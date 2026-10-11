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
    pub fn with_adapt_batch_usize(mut self, batch: usize) -> Self {
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
            Self::InvalidEwmaAlpha(a) => {
                write!(f, "ewma_alpha must be in (0, 1], got {a}")
            }
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
        assert_eq!(
            AdaptiveLimitConfig::default()
                .build()
                .unwrap()
                .current_limit(),
            16
        );
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
}
