use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use pin_project_lite::pin_project;
use tower::Service;

use crate::{error::AdaptiveLimitError, permit::Permit, state::AdaptiveLimit};

pub(crate) type Classifier<R> = Arc<dyn Fn(&R) -> bool + Send + Sync + 'static>;

pin_project! {
    #[project = StateProj]
    enum State<F, R> {
        Rejected,
        InFlight {
            #[pin]
            fut: F,
            permit: Permit,
            classifier: Classifier<R>
        }
    }
}

pin_project! {
    pub struct ResponseFuture<F, R> {
        #[pin]
        state: State<F, R>
    }
}

impl<F, R> ResponseFuture<F, R> {
    pub(crate) fn rejected() -> Self {
        Self {
            state: State::Rejected,
        }
    }

    pub(crate) fn in_flight(fut: F, permit: Permit, classifier: Classifier<R>) -> Self {
        Self {
            state: State::InFlight {
                fut,
                permit,
                classifier,
            },
        }
    }
}

impl<F, R, E> Future for ResponseFuture<F, R>
where
    F: Future<Output = Result<R, E>>,
{
    type Output = Result<R, AdaptiveLimitError<E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            StateProj::Rejected => Poll::Ready(Err(AdaptiveLimitError::LimitExceeded)),
            StateProj::InFlight {
                fut,
                permit,
                classifier,
            } => {
                let result = ready!(fut.poll(cx));
                let rtt = permit.elapsed();

                match result {
                    Ok(response) => {
                        let is_success = classifier.as_ref()(&response);
                        permit.complete_with(rtt, is_success);
                        Poll::Ready(Ok(response))
                    }
                    Err(err) => {
                        permit.complete_with(rtt, false);
                        Poll::Ready(Err(AdaptiveLimitError::Inner(err)))
                    }
                }
            }
        }
    }
}

impl<F, R> fmt::Debug for ResponseFuture<F, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseFuture").finish_non_exhaustive()
    }
}

pub struct AdaptiveLimitService<S, R> {
    inner: S,
    limiter: AdaptiveLimit,
    classifier: Classifier<R>,
}

impl<S, R> AdaptiveLimitService<S, R> {
    pub(crate) fn new(inner: S, limiter: AdaptiveLimit, classifier: Classifier<R>) -> Self {
        Self {
            inner,
            limiter,
            classifier,
        }
    }

    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    pub fn limiter(&self) -> &AdaptiveLimit {
        &self.limiter
    }
}

impl<S, Request, R> Service<Request> for AdaptiveLimitService<S, R>
where
    S: Service<Request, Response = R>,
{
    type Response = R;
    type Error = AdaptiveLimitError<S::Error>;
    type Future = ResponseFuture<S::Future, R>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(AdaptiveLimitError::Inner)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        if !self.limiter.shared().try_acquire() {
            return ResponseFuture::rejected();
        }

        let permit = Permit::new(Arc::clone(self.limiter.shared()));

        let fut = self.inner.call(req);

        ResponseFuture::in_flight(fut, permit, Arc::clone(&self.classifier))
    }
}

impl<S: Clone, R> Clone for AdaptiveLimitService<S, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            limiter: self.limiter.clone(),
            classifier: Arc::clone(&self.classifier),
        }
    }
}

impl<S: fmt::Debug, R> fmt::Debug for AdaptiveLimitService<S, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdaptiveLimitService")
            .field("inner", &self.inner)
            .field("limiter", &self.limiter)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AdaptiveLimitConfig;
    use std::convert::Infallible;

    #[derive(Clone, Debug)]
    struct OkService;

    impl Service<u32> for OkService {
        type Response = u32;
        type Error = Infallible;
        type Future = std::future::Ready<Result<u32, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: u32) -> Self::Future {
            std::future::ready(Ok(req))
        }
    }

    #[derive(Clone, Debug)]
    struct ErrService;

    impl Service<u32> for ErrService {
        type Response = u32;
        type Error = &'static str;
        type Future = std::future::Ready<Result<u32, &'static str>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: u32) -> Self::Future {
            std::future::ready(Err("failed"))
        }
    }

    #[derive(Clone, Debug)]
    struct NotReadyErrService;

    impl Service<u32> for NotReadyErrService {
        type Response = u32;
        type Error = &'static str;
        type Future = std::future::Ready<Result<u32, &'static str>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Err("poll error"))
        }

        fn call(&mut self, _req: u32) -> Self::Future {
            std::future::ready(Ok(0))
        }
    }

    #[derive(Clone)]
    struct PendingOnceService;

    impl Service<u32> for PendingOnceService {
        type Response = u32;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<u32, Infallible>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: u32) -> Self::Future {
            Box::pin(async move {
                tokio::task::yield_now().await;
                Ok(req)
            })
        }
    }

    #[tokio::test]
    async fn successful_call_flow() {
        let limiter = AdaptiveLimit::new(AdaptiveLimitConfig::new()).unwrap();
        let classifier: Classifier<u32> = Arc::new(|res| *res > 0);
        let mut svc = AdaptiveLimitService::new(OkService, limiter.clone(), classifier);

        std::future::poll_fn(|cx| svc.poll_ready(cx)).await.unwrap();

        assert_eq!(svc.call(42).await.unwrap(), 42);

        let snap = limiter.snapshot();
        assert_eq!(snap.total_completed, 1);
        assert_eq!(snap.total_errors, 0);
        assert_eq!(snap.active_requests, 0);
    }

    #[tokio::test]
    async fn inner_service_failure_flow() {
        let limiter = AdaptiveLimit::new(AdaptiveLimitConfig::new()).unwrap();
        let mut svc =
            AdaptiveLimitService::new(ErrService, limiter.clone(), Arc::new(|_: &u32| true));

        std::future::poll_fn(|cx| svc.poll_ready(cx)).await.unwrap();
        let err = svc.call(1).await.unwrap_err();
        assert_eq!(err, AdaptiveLimitError::Inner("failed"));

        let snap = limiter.snapshot();
        assert_eq!(snap.total_completed, 1);
        assert_eq!(snap.total_errors, 1);
        assert_eq!(snap.active_requests, 0);
    }

    #[tokio::test]
    async fn classifier_rejection_marks_error() {
        let limiter = AdaptiveLimit::new(AdaptiveLimitConfig::new()).unwrap();
        let mut svc =
            AdaptiveLimitService::new(OkService, limiter.clone(), Arc::new(|res: &u32| *res != 0));

        assert_eq!(svc.call(0).await.unwrap(), 0);

        let snap = limiter.snapshot();
        assert_eq!(snap.total_completed, 1);
        assert_eq!(snap.total_errors, 1);
    }

    #[tokio::test]
    async fn limit_exceeded_rejected() {
        let limiter = AdaptiveLimit::new(
            AdaptiveLimitConfig::new()
                .with_min_limit(1)
                .with_max_limit(1)
                .with_initial_limit(1)
                .with_adapt_batch_size(1_000_000),
        )
        .unwrap();

        let held = limiter.try_acquire_permit().unwrap();
        let mut svc =
            AdaptiveLimitService::new(OkService, limiter.clone(), Arc::new(|_: &u32| true));

        let err = svc.call(10).await.unwrap_err();
        assert!(err.is_limit_exceeded());
        assert!(err.into_inner().is_none());
        assert_eq!(limiter.snapshot().total_rejected, 1);
        assert_eq!(limiter.snapshot().active_requests, 1);

        drop(held);
        assert_eq!(limiter.snapshot().active_requests, 0);
    }

    #[tokio::test]
    async fn pending_inner_future_polls_until_ready() {
        let limiter = AdaptiveLimit::new(AdaptiveLimitConfig::new()).unwrap();
        let mut svc =
            AdaptiveLimitService::new(PendingOnceService, limiter.clone(), Arc::new(|_: &u32| true));

        std::future::poll_fn(|cx| svc.poll_ready(cx)).await.unwrap();
        let fut = svc.call(7);
        assert_eq!(limiter.snapshot().active_requests, 1);

        assert_eq!(fut.await.unwrap(), 7);

        let snap = limiter.snapshot();
        assert_eq!(snap.active_requests, 0);
        assert_eq!(snap.total_completed, 1);
        assert_eq!(snap.total_dropped, 0);
    }

    #[tokio::test]
    async fn dropped_in_flight_future_cancels_permit() {
        let limiter = AdaptiveLimit::new(
            AdaptiveLimitConfig::new()
                .with_min_limit(1)
                .with_max_limit(1)
                .with_initial_limit(1)
                .with_adapt_batch_size(1_000_000),
        )
        .unwrap();
        let mut svc =
            AdaptiveLimitService::new(PendingOnceService, limiter.clone(), Arc::new(|_: &u32| true));

        let fut = svc.call(1);
        assert_eq!(limiter.snapshot().active_requests, 1);

        drop(fut);

        let snap = limiter.snapshot();
        assert_eq!(snap.active_requests, 0);
        assert_eq!(snap.total_dropped, 1);
        assert_eq!(snap.total_completed, 0);
        assert_eq!(snap.min_rtt, None);
        assert_eq!(snap.ewma_rtt, None);

        assert_eq!(svc.call(2).await.unwrap(), 2);
        assert_eq!(limiter.snapshot().active_requests, 0);
    }

    #[tokio::test]
    async fn poll_ready_maps_inner_error() {
        let limiter = AdaptiveLimit::new(AdaptiveLimitConfig::new()).unwrap();
        let mut svc =
            AdaptiveLimitService::new(NotReadyErrService, limiter, Arc::new(|_: &u32| true));

        let err = std::future::poll_fn(|cx| svc.poll_ready(cx)).await.unwrap_err();
        assert_eq!(err, AdaptiveLimitError::Inner("poll error"));
        assert_eq!(svc.call(0).await.unwrap(), 0);
    }

    #[test]
    fn service_inspectors_and_clones() {
        let limiter = AdaptiveLimit::new(AdaptiveLimitConfig::new()).unwrap();
        let mut svc =
            AdaptiveLimitService::new(OkService, limiter.clone(), Arc::new(|_: &u32| true));

        let _ = svc.get_ref();
        let _ = svc.get_mut();
        assert_eq!(svc.limiter().current_limit(), limiter.current_limit());

        let cloned = svc.clone();
        assert_eq!(cloned.limiter().current_limit(), limiter.current_limit());

        let debug_str = format!("{svc:?}");
        assert!(debug_str.contains("AdaptiveLimitService"));

        let _inner: OkService = svc.into_inner();
    }

    #[test]
    fn response_future_debug() {
        let fut: ResponseFuture<std::future::Ready<Result<(), ()>>, ()> =
            ResponseFuture::rejected();
        assert!(format!("{fut:?}").contains("ResponseFuture"));
    }
}
