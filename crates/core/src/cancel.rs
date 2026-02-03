use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use crate::error::{RusticJobError, RusticJobResult};
use crate::{Progress, ProgressBars};

#[derive(Clone, Default, Debug)]
pub struct JobCancelToken {
    inner: Arc<AtomicBool>,
}

impl JobCancelToken {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn cancel(&self) {
        self.inner.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.load(Ordering::Acquire)
    }
    
    pub(crate) fn ensure_check(&self) -> RusticJobResult<()> {
        if self.is_cancelled() {
            Err(RusticJobError::JobCancelled)
        } else {
            Ok(())
        }
    }

    pub(crate) fn ensure_good(&self, p: &impl Progress) -> RusticJobResult<()> {
        if self.is_cancelled() {
            //p.set_title("job cancelled");
            p.finish();
            Err(RusticJobError::JobCancelled)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn cancel_is_visible_across_clones() {
        let token = JobCancelToken::new();
        let a = token.clone();
        let b = token.clone();

        assert!(!a.is_cancelled());
        assert!(!b.is_cancelled());

        token.cancel();

        assert!(a.is_cancelled());
        assert!(b.is_cancelled());
    }

    #[test]
    fn worker_thread_exits_on_cancel() {
        let token = JobCancelToken::new();
        let worker_token = token.clone();

        let handle = thread::spawn(move || {
            while !worker_token.is_cancelled() {
                thread::yield_now();
            }
            42
        });

        thread::sleep(Duration::from_millis(50));
        token.cancel();

        let result = handle.join().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn multiple_threads_see_cancellation() {
        let token = JobCancelToken::new();
        let mut handles = Vec::new();

        for _ in 0..8 {
            let t = token.clone();
            handles.push(thread::spawn(move || {
                while !t.is_cancelled() {
                    thread::yield_now();
                }
                true
            }));
        }

        thread::sleep(Duration::from_millis(50));
        token.cancel();

        for h in handles {
            assert!(h.join().unwrap());
        }
    }

    #[test]
    fn cancellation_is_fast_and_shared() {
        let token = JobCancelToken::new();
        let t = token.clone();

        let start = Instant::now();
        let handle = thread::spawn(move || {
            while !t.is_cancelled() {}
            start.elapsed()
        });

        thread::sleep(Duration::from_millis(10));
        token.cancel();

        let elapsed = handle.join().unwrap();
        assert!(elapsed < Duration::from_secs(1));
    }
}
