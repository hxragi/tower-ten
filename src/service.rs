use std::{
    fmt,
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
