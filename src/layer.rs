use std::{fmt, sync::Arc};

use tower::Layer;

use crate::{
    service::{AdaptiveLimitService, Classifier},
    state::AdaptiveLimit,
};

pub struct AdaptiveLimitLayer<R> {
    limiter: AdaptiveLimit,
    classifier: Classifier<R>,
}

impl<R> AdaptiveLimitLayer<R> {
    pub fn new(limiter: AdaptiveLimit) -> Self {
        Self {
            limiter,
            classifier: Arc::new(|_: &R| true),
        }
    }

    pub fn with_classifier<F>(mut self, classifier: F) -> Self
    where
        F: Fn(&R) -> bool + Send + Sync + 'static,
    {
        self.classifier = Arc::new(classifier);
        self
    }

    pub fn limiter(&self) -> &AdaptiveLimit {
        &self.limiter
    }
}

impl<R> Clone for AdaptiveLimitLayer<R> {
    fn clone(&self) -> Self {
        Self {
            limiter: self.limiter.clone(),
            classifier: Arc::clone(&self.classifier),
        }
    }
}

impl<S, R> Layer<S> for AdaptiveLimitLayer<R> {
    type Service = AdaptiveLimitService<S, R>;

    fn layer(&self, inner: S) -> Self::Service {
        AdaptiveLimitService::new(inner, self.limiter.clone(), Arc::clone(&self.classifier))
    }
}

impl<R> fmt::Debug for AdaptiveLimitLayer<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdaptiveLimitLayer")
            .field("limiter", &self.limiter)
            .finish_non_exhaustive()
    }
}
