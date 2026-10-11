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
