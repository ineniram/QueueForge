# Engineering decisions

## Go and independent workers

Go provides a small runtime footprint, context-aware I/O, and compiled worker binaries. Each worker container has its own process and runs one job at a time. The handler concurrency is explicit, so scaling the container count changes the number of independent executors. Cancellation and graceful shutdown use Go contexts. PostgreSQL and Redis client libraries handle their protocols, while the API uses the standard HTTP server.

## Redis dispatch and PostgreSQL state

Redis provides a sorted set of eligible job IDs and a blocking pop. This keeps idle workers from polling PostgreSQL for every job. PostgreSQL durably stores job payloads, status, attempt history, and worker generations. A reconciliation loop republishes eligible PostgreSQL rows after publication failures or lost Redis contents. The tradeoff is possible duplicate dispatch and a delay before reconciliation. It also avoids a cross-system transaction.

## Claims, leases, and fencing

An atomic PostgreSQL update changes a queued row to running only when it is eligible and has attempt budget. The winning claim records the worker generation, a deadline, and a per-job fencing token. Renewal and completion lock the row and verify that the generation, token, and deadline still confer write authority. A current heartbeat is evidence about a worker's presence. A current lease grants authority over a particular job.

## Execution and recovery

Redis delivery can repeat, and an attempt can execute again after a crash or a failed completion write. QueueForge offers at-least-once execution within a bounded attempt budget. Fencing protects database writes even if an expired worker is still executing. The built-in handlers have no external writes. An external integration would need its own idempotency key or a downstream service that accepts fencing tokens.

Recovery scans expired rows using row locks and `SKIP LOCKED`. It changes the status and writes audit events in the same PostgreSQL transaction. Jobs with budget wait for capped exponential backoff before becoming eligible again. Jobs without budget are dead-lettered and retain their history. Manual retry starts a new attempt budget without resetting the lifetime attempt count or fencing token.

## Benchmark design and production limits

The benchmark intentionally exercises the normal API and both stores. It tests a bounded workload on one machine and shares Docker VM resources across the API, databases, and workers. It measures lease occupancy and observed end-to-end performance. CPU attribution, storage I/O, and service contention require profiling before assigning a specific bottleneck.

The current stack assumes trusted local access. It lacks authentication, namespace isolation, retention and pruning, external-effect deduplication, and multi-host high availability. The polling dispatcher and aggregate API queries prioritize correctness and simplicity at this scale. Benchmark distributions and recovery timings describe the measured workload and environment.
