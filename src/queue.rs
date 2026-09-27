use async_trait::async_trait;
use chrono::Utc;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::job::{JobWrapper, JobStatus};
use crate::stats::StatsTracker;
use crate::Result;

/// Queue backend trait
#[async_trait]
pub trait QueueBackend: Send + Sync {
    /// Enqueue a job
    async fn enqueue(&self, job: JobWrapper) -> Result<()>;
    /// Dequeue the next available job
    async fn dequeue(&self) -> Result<Option<JobWrapper>>;
    /// Mark a job as completed
    async fn complete(&self, job_id: &str) -> Result<()>;
    /// Mark a job as failed
    async fn fail(&self, job_id: &str, error: String) -> Result<()>;
    /// Retry a job
    async fn retry(&self, job: JobWrapper) -> Result<()>;
    /// Move a job to the dead letter queue
    async fn move_to_dead_letter(&self, job: JobWrapper) -> Result<()>;
    /// List jobs in the dead letter queue
    async fn list_dead_letter(&self) -> Result<Vec<JobWrapper>>;
    /// Retry a job from the dead letter queue
    async fn retry_from_dead_letter(&self, job_id: &str) -> Result<()>;
    /// Clear all jobs from the queue
    async fn clear(&self) -> Result<()>;
}

/// In-memory queue backend.
///
/// Ready jobs live in a max-heap ordered by priority with FIFO tiebreaks;
/// future-scheduled jobs wait in a min-heap ordered by due time. Both
/// enqueue and dequeue cost O(log n) instead of the O(n) scan and memmove
/// of an ordered vector, while preserving the original semantics: highest
/// priority first among due jobs, FIFO within equal priority.
pub struct MemoryBackend {
    inner: Arc<Mutex<MemoryQueue>>,
    dead_letter: Arc<Mutex<Vec<JobWrapper>>>,
}

#[derive(Default)]
struct MemoryQueue {
    ready: BinaryHeap<ReadyJob>,
    delayed: BinaryHeap<DelayedJob>,
    seq: u64,
}

struct ReadyJob {
    priority: i32,
    seq: u64,
    job: JobWrapper,
}

impl PartialEq for ReadyJob {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}

impl Eq for ReadyJob {}

impl PartialOrd for ReadyJob {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReadyJob {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority first; earlier insertion first on ties.
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

struct DelayedJob {
    due: i64,
    seq: u64,
    job: JobWrapper,
}

impl PartialEq for DelayedJob {
    fn eq(&self, other: &Self) -> bool {
        self.due == other.due && self.seq == other.seq
    }
}

impl Eq for DelayedJob {}

impl PartialOrd for DelayedJob {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DelayedJob {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap by due time; earlier insertion first on ties.
        other
            .due
            .cmp(&self.due)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl MemoryBackend {
    /// Create a new in-memory queue backend
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryQueue::default())),
            dead_letter: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QueueBackend for MemoryBackend {
    async fn enqueue(&self, mut job: JobWrapper) -> Result<()> {
        job.status = JobStatus::Pending;
        let mut inner = self.inner.lock().await;
        let seq = inner.seq;
        inner.seq = inner.seq.wrapping_add(1);

        let now = Utc::now().timestamp();
        match job.scheduled_at {
            Some(due) if due > now => inner.delayed.push(DelayedJob { due, seq, job }),
            _ => inner.ready.push(ReadyJob {
                priority: job.priority,
                seq,
                job,
            }),
        }

        Ok(())
    }

    async fn dequeue(&self) -> Result<Option<JobWrapper>> {
        let mut inner = self.inner.lock().await;

        // Promote due jobs before selecting, so scheduled jobs become
        // visible exactly when their time arrives.
        let now = Utc::now().timestamp();
        while let Some(top) = inner.delayed.peek() {
            if top.due > now {
                break;
            }
            let delayed = inner.delayed.pop().expect("peeked entry exists");
            inner.ready.push(ReadyJob {
                priority: delayed.job.priority,
                seq: delayed.seq,
                job: delayed.job,
            });
        }

        if let Some(ready) = inner.ready.pop() {
            let mut job = ready.job;
            job.status = JobStatus::Running;
            job.attempts += 1;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    async fn complete(&self, _job_id: &str) -> Result<()> {
        // In memory backend doesn't need to track completed jobs
        Ok(())
    }

    async fn fail(&self, _job_id: &str, _error: String) -> Result<()> {
        // In memory backend doesn't need to track failed jobs
        Ok(())
    }

    async fn retry(&self, job: JobWrapper) -> Result<()> {
        self.enqueue(job).await
    }

    async fn move_to_dead_letter(&self, mut job: JobWrapper) -> Result<()> {
        job.status = JobStatus::DeadLetter;
        let mut dlq = self.dead_letter.lock().await;
        dlq.push(job);
        Ok(())
    }

    async fn list_dead_letter(&self) -> Result<Vec<JobWrapper>> {
        let dlq = self.dead_letter.lock().await;
        Ok(dlq.clone())
    }

    async fn retry_from_dead_letter(&self, job_id: &str) -> Result<()> {
        let mut dlq = self.dead_letter.lock().await;
        if let Some(pos) = dlq.iter().position(|j| j.id == job_id) {
            let mut job = dlq.remove(pos);
            job.status = JobStatus::Pending;
            job.attempts = 0;
            job.error = None;
            drop(dlq); // Release lock before enqueue
            self.enqueue(job).await?;
        }
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.ready.clear();
        inner.delayed.clear();
        Ok(())
    }
}

/// Queue for managing jobs
pub struct Queue {
    backend: Arc<dyn QueueBackend>,
    stats: StatsTracker,
}

impl Queue {
    /// Create a new queue with the given backend
    pub fn new(backend: Arc<dyn QueueBackend>) -> Self {
        Self {
            backend,
            stats: StatsTracker::new(),
        }
    }

    /// Create a new queue backed by an in-memory backend
    pub fn memory() -> Self {
        Self::new(Arc::new(MemoryBackend::new()))
    }

    /// Enqueue a job and return its ID
    pub async fn enqueue(&self, job: JobWrapper) -> Result<String> {
        let job_id = job.id.clone();
        self.backend.enqueue(job).await?;
        self.stats.increment_enqueued().await;
        Ok(job_id)
    }

    /// Dequeue the next available job
    pub async fn dequeue(&self) -> Result<Option<JobWrapper>> {
        let job = self.backend.dequeue().await?;
        if job.is_some() {
            self.stats.mark_running().await;
        }
        Ok(job)
    }

    /// Mark a job as completed
    pub async fn complete(&self, job_id: &str) -> Result<()> {
        self.backend.complete(job_id).await?;
        self.stats.increment_processed().await;
        Ok(())
    }

    /// Mark a job as failed
    pub async fn fail(&self, job_id: &str, error: String) -> Result<()> {
        self.backend.fail(job_id, error).await?;
        self.stats.increment_failed().await;
        Ok(())
    }

    /// Retry a job
    pub async fn retry(&self, job: JobWrapper) -> Result<()> {
        self.backend.retry(job).await?;
        self.stats.increment_retried().await;
        Ok(())
    }

    /// Move a job to the dead letter queue
    pub async fn move_to_dead_letter(&self, job: JobWrapper) -> Result<()> {
        self.backend.move_to_dead_letter(job).await?;
        self.stats.increment_dead_letter().await;
        Ok(())
    }

    /// List jobs in the dead letter queue
    pub async fn list_dead_letter(&self) -> Result<Vec<JobWrapper>> {
        self.backend.list_dead_letter().await
    }

    /// Retry a job from the dead letter queue
    pub async fn retry_from_dead_letter(&self, job_id: &str) -> Result<()> {
        self.backend.retry_from_dead_letter(job_id).await
    }

    /// Clear all jobs from the queue
    pub async fn clear(&self) -> Result<()> {
        self.backend.clear().await?;
        self.stats.reset().await;
        Ok(())
    }

    /// Get current queue statistics
    pub async fn get_stats(&self) -> crate::stats::QueueStats {
        self.stats.get_stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Job;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct TestJob {
        value: i32,
    }

    #[async_trait::async_trait]
    impl Job for TestJob {
        async fn perform(&self) -> crate::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_enqueue_dequeue() {
        let queue = Queue::memory();
        let job = JobWrapper::new(&TestJob { value: 42 }).unwrap();
        
        queue.enqueue(job).await.unwrap();
        let dequeued = queue.dequeue().await.unwrap();
        
        assert!(dequeued.is_some());
    }

    #[derive(Serialize, Deserialize)]
    struct PriorityJob {
        value: i32,
        prio: i32,
    }

    #[async_trait::async_trait]
    impl Job for PriorityJob {
        async fn perform(&self) -> crate::Result<()> {
            Ok(())
        }

        fn priority(&self) -> i32 {
            self.prio
        }
    }

    async fn dequeue_value(backend: &MemoryBackend) -> Option<i32> {
        backend
            .dequeue()
            .await
            .unwrap()
            .map(|job| {
                serde_json::from_value::<PriorityJob>(job.payload)
                    .expect("test payload decodes")
                    .value
            })
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let backend = MemoryBackend::new();
        for (value, prio) in [(1, 0), (2, 10), (3, 5)] {
            backend
                .enqueue(JobWrapper::new(&PriorityJob { value, prio }).unwrap())
                .await
                .unwrap();
        }

        assert_eq!(dequeue_value(&backend).await, Some(2));
        assert_eq!(dequeue_value(&backend).await, Some(3));
        assert_eq!(dequeue_value(&backend).await, Some(1));
        assert_eq!(dequeue_value(&backend).await, None);
    }

    #[tokio::test]
    async fn test_fifo_within_equal_priority() {
        let backend = MemoryBackend::new();
        for value in [1, 2, 3] {
            backend
                .enqueue(JobWrapper::new(&PriorityJob { value, prio: 0 }).unwrap())
                .await
                .unwrap();
        }

        assert_eq!(dequeue_value(&backend).await, Some(1));
        assert_eq!(dequeue_value(&backend).await, Some(2));
        assert_eq!(dequeue_value(&backend).await, Some(3));
    }

    #[tokio::test]
    async fn test_delayed_job_waits_for_due_time() {
        let backend = MemoryBackend::new();
        let mut delayed =
            JobWrapper::new(&PriorityJob { value: 99, prio: 100 }).unwrap();
        delayed.scheduled_at = Some(chrono::Utc::now().timestamp() + 3600);
        backend.enqueue(delayed).await.unwrap();

        // Not due: nothing available even though priority is highest.
        assert_eq!(dequeue_value(&backend).await, None);

        // A ready job dequeues while the delayed one waits.
        backend
            .enqueue(JobWrapper::new(&PriorityJob { value: 1, prio: 0 }).unwrap())
            .await
            .unwrap();
        assert_eq!(dequeue_value(&backend).await, Some(1));
        assert_eq!(dequeue_value(&backend).await, None);
    }
}
