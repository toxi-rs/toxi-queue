# toxi-queue

Background jobs with retries, cron, and a dead-letter queue. Memory
and Redis backends.

```toml
[dependencies]
toxi-queue = "3"
```

```rust
use toxi_queue::*;

let queue = Queue::memory();
let job = JobWrapper::new(&SendEmailJob {
    to: "user@example.com".into(),
})?;
queue.enqueue(job).await?;

let worker = Worker::new(Arc::new(queue)).worker_count(4);
worker.start().await;
```
