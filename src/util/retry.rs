use std::error::Error;
use std::future::Future;
use std::time::Duration;
use tracing::warn;

/// Static retry function for retrying operations
pub async fn retry_with_max_retries<F, Fut, T, E>(
    max_retries: usize,
    operation_name: &str,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Error + Send + Sync,
{
    let mut last_error = None;

    for attempt in 0..=max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                // Check if error is retryable (connection errors)
                let error_msg = format!("{:?}", e);
                let is_retryable = error_msg.contains("ConnectionReset")
                    || error_msg.contains("BrokenPipe")
                    || error_msg.contains("Interrupted")
                    || error_msg.contains("TimedOut");

                if !is_retryable || attempt == max_retries {
                    return Err(e);
                }

                warn!(
                    "Retryable error in {} (attempt {}/{}): {:?}",
                    operation_name,
                    attempt + 1,
                    max_retries,
                    e
                );

                last_error = Some(e);

                // Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms, ...
                let backoff_ms = 100 * (1 << attempt.min(10));
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }

    Err(last_error.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Custom error type for testing
    #[derive(Debug, Clone)]
    struct TestError {
        message: String,
    }

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.message)
        }
    }

    impl Error for TestError {}

    #[tokio::test]
    async fn test_retry_success_on_first_attempt() {
        let result =
            retry_with_max_retries(3, "test_operation", || async { Ok::<i32, TestError>(42) })
                .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_success_after_retries() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = retry_with_max_retries(5, "test_operation", move || {
            let counter = Arc::clone(&counter_clone);
            async move {
                let count = counter.fetch_add(1, Ordering::SeqCst);
                if count < 2 {
                    // Fail first 2 attempts with retryable error
                    Err(TestError {
                        message: "ConnectionReset error".to_string(),
                    })
                } else {
                    Ok(100)
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);
        // Should have been called 3 times (2 failures + 1 success)
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_non_retryable_error() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = retry_with_max_retries(5, "test_operation", move || {
            let counter = Arc::clone(&counter_clone);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<i32, TestError>(TestError {
                    message: "NotFound error".to_string(),
                })
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().message, "NotFound error");
        // Should only be called once (non-retryable error)
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_max_retries_exceeded() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = retry_with_max_retries(3, "test_operation", move || {
            let counter = Arc::clone(&counter_clone);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<i32, TestError>(TestError {
                    message: "TimedOut error".to_string(),
                })
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().message, "TimedOut error");
        // Should be called max_retries + 1 times (0..=3 = 4 attempts)
        assert_eq!(counter.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn test_retry_broken_pipe_error() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = retry_with_max_retries(3, "test_operation", move || {
            let counter = Arc::clone(&counter_clone);
            async move {
                let count = counter.fetch_add(1, Ordering::SeqCst);
                if count < 1 {
                    Err(TestError {
                        message: "BrokenPipe error".to_string(),
                    })
                } else {
                    Ok(200)
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);
    }

    #[tokio::test]
    async fn test_retry_interrupted_error() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = retry_with_max_retries(3, "test_operation", move || {
            let counter = Arc::clone(&counter_clone);
            async move {
                let count = counter.fetch_add(1, Ordering::SeqCst);
                if count < 1 {
                    Err(TestError {
                        message: "Interrupted error".to_string(),
                    })
                } else {
                    Ok(300)
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 300);
    }

    #[tokio::test]
    async fn test_retry_zero_max_retries() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = retry_with_max_retries(0, "test_operation", move || {
            let counter = Arc::clone(&counter_clone);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<i32, TestError>(TestError {
                    message: "ConnectionReset error".to_string(),
                })
            }
        })
        .await;

        assert!(result.is_err());
        // Should only be called once (max_retries = 0 means 1 attempt)
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_exponential_backoff() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let start = std::time::Instant::now();

        let result = retry_with_max_retries(2, "test_operation", move || {
            let counter = Arc::clone(&counter_clone);
            async move {
                let count = counter.fetch_add(1, Ordering::SeqCst);
                if count < 2 {
                    Err(TestError {
                        message: "TimedOut error".to_string(),
                    })
                } else {
                    Ok(400)
                }
            }
        })
        .await;

        let elapsed = start.elapsed();

        assert!(result.is_ok());
        // Should have waited: 100ms (after 1st retry) + 200ms (after 2nd retry) = 300ms minimum
        assert!(elapsed.as_millis() >= 250); // Allow some tolerance
    }
}
