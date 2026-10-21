# reliable-events

Effectively-once Kafka consumption and a transactional outbox for Spring Boot services.

A personal reference project: the patterns I rely on when an event-driven service has to stay
correct under redelivery, broker outages and crashes. Not production software and not derived
from any employer's code.

## Status

| Module | State |
|---|---|
| `reliable-events-core` | Done: idempotency, retries with jitter, dead-lettering, backpressure, outbox relay |
| `reliable-events-spring` | Next: PostgreSQL stores, Spring Kafka adapters, auto-configuration |
| `example-order-service` | Planned: runnable demo with Docker Compose |

## Core ideas

- **At-least-once in, effectively-once out.** Every message is claimed by id before processing.
  Redeliveries of completed messages are skipped; a crashed worker's claim expires so the message
  is not lost.
- **Poison messages do not loop.** Retryable failures back off with full jitter; exhausted or
  non-retryable failures go to a dead-letter topic and are marked done. If the dead-letter publish
  itself fails, the claim is released and the error rethrown, so the broker redelivers.
- **Backpressure with hysteresis.** Consumption pauses at a ceiling and resumes only below a lower
  watermark (default 70%), so it does not flap near the limit.
- **Transactional outbox.** Events are written in the business transaction and relayed afterwards
  with leasing, so a change that rolled back never emits an event and a committed one is never lost.

## Build

```bash
mvn verify
```

Requires Java 21 and Maven 3.9+.
