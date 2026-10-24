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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AdaptiveLimitConfig;
    use tower::Service;

    #[derive(Clone)]
    struct EchoService;

    impl Service<String> for EchoService {
        type Response = String;
        type Error = ();
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: String) -> Self::Future {
            std::future::ready(Ok(req))
        }
    }

    #[test]
    fn layer_constructs_and_clones() {
        let limiter = AdaptiveLimit::new(AdaptiveLimitConfig::new()).unwrap();
        let layer = AdaptiveLimitLayer::<String>::new(limiter.clone())
            .with_classifier(|res: &String| !res.is_empty());

        assert!((layer.classifier)(&"hello".to_string()));
        assert!(!(layer.classifier)(&String::new()));

        assert_eq!(layer.limiter().current_limit(), limiter.current_limit());

        let cloned = layer.clone();
        assert_eq!(cloned.limiter().current_limit(), limiter.current_limit());

        assert!(format!("{layer:?}").contains("AdaptiveLimitLayer"));
    }

    #[tokio::test]
    async fn layer_poll_ready_delegates_to_inner() {
        let limiter = AdaptiveLimit::new(AdaptiveLimitConfig::new()).unwrap();
        let mut svc = AdaptiveLimitLayer::new(limiter).layer(EchoService);

        std::future::poll_fn(|cx| svc.poll_ready(cx)).await.unwrap();
    }

    #[tokio::test]
    async fn layer_wraps_service() {
        let limiter = AdaptiveLimit::new(AdaptiveLimitConfig::new()).unwrap();
        let mut svc = AdaptiveLimitLayer::new(limiter).layer(EchoService);

        assert_eq!(svc.call("hello".to_string()).await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn layer_classifier_marks_failures() {
        let limiter =
            AdaptiveLimit::new(AdaptiveLimitConfig::new().with_adapt_batch_size(1_000_000))
                .unwrap();
        let mut svc = AdaptiveLimitLayer::new(limiter.clone())
            .with_classifier(|res: &String| !res.is_empty())
            .layer(EchoService);

        assert_eq!(svc.call("ok".to_string()).await.unwrap(), "ok");
        assert_eq!(svc.call(String::new()).await.unwrap(), "");

        let snap = limiter.snapshot();
        assert_eq!(snap.total_completed, 2);
        assert_eq!(snap.total_errors, 1);
        assert_eq!(snap.active_requests, 0);
    }
}
