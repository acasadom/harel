# InMemoryTransport — in-process

The same-process transport: a Python list of message dicts guarded by a `threading.Lock`. For
tests and single-process embedding. It has the *same lease semantics* as the durable backends
(so the deterministic `step()`-in-a-loop tests exercise the real claim logic) but does no IO —
state lives in the list and dies with the process. The lock serializes `claim` exactly the way
SQLite's write-lock does across processes, so the per-group exclusivity check is race-free.

## Data model

`self._messages: list[dict]` — one entry per queued message — plus `self._groups: dict[str, dict]`
that tracks per-group metadata (`last_claimed_at`, `priority`), a monotonic `self._seq` counter,
and a `threading.Lock`:

```text
_messages entry:
{
  "seq":         int,          # monotonic id — FIFO order AND the Lease handle (ack/nack target)
  "group_id":    str,          # the execution id — the exclusivity group
  "event":       Event,        # the Event OBJECT itself (no serialization — in-memory)
  "locked_by":   str | None,   # worker id while leased, "__parked__" while parked, None when free
  "lock_expiry": float,        # epoch when the lease/park ends (0.0 when free)
}

_groups entry:  { "last_claimed_at": float, "priority": int }   # one per active group
```

`locked_by` + `lock_expiry` together are the **lease**: a message is *in flight* while
`locked_by is not None and lock_expiry >= now`. Unlike the durable backends the `event` is kept
as the live `Event` object (no JSON round-trip), matching the in-memory store's identity contract.

## Single-active-consumer-per-group, round-robin, and priority

`claim` enforces the invariant — at most one in-flight message per group — and applies
**round-robin fairness** and an optional **priority floor** under the lock:

```text
in_flight = { m.group_id for m in messages if m.locked_by is not None and m.lock_expiry >= now }
for m in messages sorted by (groups[m.group_id].last_claimed_at ASC, seq ASC):  # oldest-claimed first
    available = m.locked_by is None or m.lock_expiry < now
    if not available or m.group_id in in_flight:          continue
    if groups[m.group_id].priority < min_priority:        continue   # below priority floor
    m.locked_by   = worker_id
    m.lock_expiry = now + visibility
    groups[m.group_id].last_claimed_at = now              # round-robin: move to back of queue
    return Lease(m.seq, m.group_id, m.event)
return None
```

Sorting by `last_claimed_at` (ascending) means groups never yet claimed (`0`) come first; after
each ack that value is set to `now`, so a just-processed group yields to others. The
`lock_expiry < now` test is the **crash recovery** path (expired lease → treated as free).

## Operations

```text
publish(group_id, event, priority=0)
    # seq += 1; append to _messages; groups.setdefault(group_id, {last_claimed_at:0, priority:priority})
    # priority is set on first publish only (setdefault ignores later calls)
claim(worker_id, visibility, min_priority=0)   # the round-robin select-then-lease above
ack(lease)    # remove message; if group now empty, remove from _groups
nack(lease, delay=0)       # delay>0 -> park: locked_by="__parked__", lock_expiry=now+delay
                           # delay==0 -> retry now: locked_by=None, lock_expiry=0.0
close()                    # no-op (the list lives with the process)
```

`ack` removes the message (its group is then free for the next message) and drops the group
entry when no messages remain. `nack(delay>0)` **parks** it: the `_PARKED` sentinel keeps the
`in_flight` set blocked until `lock_expiry` passes — the [control plane](../control-plane) uses
this for a suspended group. `nack(0)` frees it for immediate retry.

## FIFO

Messages of a group are delivered oldest-first (lowest `seq` within the round-robin sort), and a
group is only ever advanced by one consumer at a time, so within a group order is preserved.

See the [transports hub](../transports) for the contract and [distribution](../distribution) for
running workers.
