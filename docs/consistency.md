# Consistency and failures

PostgreSQL owns submission, attempt history, and lifecycle state. Redis is a disposable dispatch index containing eligible job IDs. A successful submission means PostgreSQL accepted the job. An API dispatcher reconciles missed Redis publishes. Workers can receive duplicates and claim only queued jobs whose eligibility timestamp has passed.

## Leases and worker health

Each process registers a unique startup generation using a random suffix in its worker ID. It heartbeats while idle, processing, or draining its active job on a graceful shutdown. Worker health is computed in PostgreSQL against its clock. The dispatcher records one stale event per uninterrupted heartbeat gap, with the current job ID when available. A graceful stop explicitly reports stopped and counts as offline.

Claiming atomically sets the generation, deadline, and an incremented per-job fencing token. Renew and finish lock the job row before checking the generation, token, and deadline. An expired owner cannot extend its own lease. After reassignment, the next claim has a strictly newer token. An old process may continue running temporarily, but it cannot commit a QueueForge result after losing this authority.

## Recovery, retries, and dead letters

The API scans expired running jobs under row locks with `SKIP LOCKED`, then validates expiry again before changing state. Concurrent coordinators process disjoint locked rows. The state transition and audit events commit together. The existing dispatcher republishes the recovered ID only when eligible.

Both handler errors and expired attempts consume retry budget. For a retryable attempt, the delay is the base delay doubled for each subsequent failed attempt and capped by the configured maximum. The PostgreSQL eligibility check also protects against duplicate or stale Redis entries. Workers can immediately pick up other jobs while the retry is delayed. An exhausted job enters terminal `failed` status, displayed as Dead-lettered. Manual retry atomically opens a new budget, preserving the lifetime attempt count, fencing token, and complete recorded history.

## Delivery and idempotency

| Failure window | Behavior |
| --- | --- |
| PostgreSQL fails before the insert commits | The request returns an error. A failed connection at commit time can leave the outcome ambiguous. Retrying with the same submission key resolves that ambiguity. |
| Redis loses queued IDs, or the API exits before publishing | Eligible queued jobs remain durable and are eventually republished when the services are available. |
| A worker exits between pop and claim | The job stays queued and is redispatched. |
| A worker exits after claiming | The attempt loses ownership at lease expiry. The job is scheduled again if budget remains. |
| A terminal database write fails | The existing lease protects the attempt until it expires. The handler may run again after that. |

QueueForge offers at-least-once processing within the attempt budget and assumes dependencies eventually return. Submission idempotency deduplicates single-job requests by a unique key and a canonical request hash. It does not deduplicate handler side effects, and it does not apply to demo batches or manual retry. All built-in handlers are deterministic or perform bounded waits and have no external writes. External effects would need their own repeated-execution protections.

Failure Lab freezes only the selected disposable worker's current attempt and requests a self-exit through PostgreSQL. Normal lease expiry and retry logic handle all subsequent transitions. Worker slots are fixed at build time and also require explicit Compose enablement.
