use std::{error, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveLimitError<E> {
    // Limit concurrency exceeded - request rejected without waiting
    LimitExceeded,
    // Inner service returned error
    Inner(E),
}

impl<E> AdaptiveLimitError<E> {
    pub fn is_limit_exceeded(&self) -> bool {
        matches!(self, Self::LimitExceeded)
    }

    pub fn into_inner(self) -> Option<E> {
        match self {
            Self::Inner(err) => Some(err),
            Self::LimitExceeded => None,
        }
    }
}

impl<E: fmt::Display> fmt::Display for AdaptiveLimitError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitExceeded => f.write_str("concurrency limit exceeded"),
            Self::Inner(err) => write!(f, "inner service error: {err}"),
        }
    }
}

impl<E> error::Error for AdaptiveLimitError<E>
where
    E: fmt::Debug + fmt::Display + error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Inner(err) => Some(err),
            Self::LimitExceeded => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl error::Error for TestError {}

    fn limit_exceeded() -> AdaptiveLimitError<TestError> {
        AdaptiveLimitError::LimitExceeded
    }

    fn inner(msg: &'static str) -> AdaptiveLimitError<TestError> {
        AdaptiveLimitError::Inner(TestError(msg))
    }

    #[test]
    fn is_limit_exceeded_only_for_limit_variant() {
        assert!(limit_exceeded().is_limit_exceeded());
        assert!(!inner("boom").is_limit_exceeded());
    }

    #[test]
    fn into_inner_unwraps_only_inner_variant() {
        assert_eq!(inner("boom").into_inner(), Some(TestError("boom")));
        assert_eq!(limit_exceeded().into_inner(), None);
    }

    #[test]
    fn display_renders_both_variants() {
        assert_eq!(limit_exceeded().to_string(), "concurrency limit exceeded");
        assert_eq!(inner("boom").to_string(), "inner service error: boom");
    }

    #[test]
    fn source_exposes_inner_error_only() {
        use error::Error as _;

        assert!(limit_exceeded().source().is_none());
        let err = inner("boom");
        let source = err.source().expect("inner error must be a source");
        assert_eq!(source.to_string(), "boom");
    }

    #[test]
    fn source_survives_trait_object_erasure() {
        let err: Box<dyn error::Error> = Box::new(inner("boom"));
        let source = err.source().expect("inner error must be a source");
        assert!(source.downcast_ref::<TestError>().is_some());

        let rejected: Box<dyn error::Error> = Box::new(limit_exceeded());
        assert!(rejected.source().is_none());
    }

    #[test]
    fn derives_debug_clone_partial_eq_and_eq() {
        let err = inner("boom");
        assert_eq!(err.clone(), err);
        assert_ne!(err, inner("other"));
        assert_eq!(limit_exceeded(), limit_exceeded());
        assert_ne!(format!("{err:?}"), format!("{:?}", limit_exceeded()));
    }

    #[test]
    fn display_only_requires_display_not_error() {
        struct DisplayOnly;

        impl fmt::Display for DisplayOnly {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("display-only")
            }
        }

        let err = AdaptiveLimitError::Inner(DisplayOnly);
        assert_eq!(err.to_string(), "inner service error: display-only");
    }
}
