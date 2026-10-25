# Producer–Consumer Problem — Email System (OS Assignment 2)

**Name:** Rohini Shende  **UID:** 24003173 **Class:** 5th Sem, Section B

## Problem Statement
Producers (users) compose emails and place them in a bounded Outbox (buffer of size 5).
Consumers (mail servers) remove emails from the Outbox and send them.
Producer waits when Outbox is full; consumer waits when Outbox is empty.

## Synchronization Used
| Semaphore | Initial value | Purpose |
|-----------|---------------|---------|
| mutex | 1 | Mutual exclusion on buffer |
| empty | 5 | Counts empty slots (producer blocks when buffer full) |
| full  | 0 | Counts filled slots (consumer blocks when buffer empty / not-empty condition) |

### Producer
```
wait(empty); wait(mutex);
  add email to buffer
signal(mutex); signal(full);
```
### Consumer
```
wait(full); wait(mutex);
  remove email from buffer
signal(mutex); signal(empty);
```

## Race Condition
`race_condition_demo.c` increments a shared counter from two threads.
Without mutex, `total_sent++` (read, add, write) gets interleaved and updates are lost.
With mutex the result is correct.

## How to Run (Linux / WSL)
```
gcc email_producer_consumer.c -o email_pc -pthread
gcc race_condition_demo.c -o race -pthread
./race
./email_pc
```

## Files
- email_producer_consumer.c – main solution
- race_condition_demo.c – race condition demo
