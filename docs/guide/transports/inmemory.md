# InMemoryTransport — in-process

The same-process transport: a Python list of message dicts guarded by a `threading.Lock`. For
tests and single-process embedding. It has the *same lease semantics* as the durable backends
(so the deterministic `step()`-in-a-loop tests exercise the real claim logic) but does no IO —
state lives in the list and dies with the process. The lock serializes `claim` exactly the way
SQLite's write-lock does across processes, so the per-group exclusivity check is race-free.

## Data model

One in-memory list, `self._messages: list[dict]`, plus a monotonic `self._seq` counter and a
`threading.Lock`. Each message is a dict:

```text
{
  "seq":         int,          # monotonic id — FIFO order AND the Lease handle (ack/nack target)
  "group_id":    str,          # the execution id — the exclusivity group
  "event":       Event,        # the Event OBJECT itself (no serialization — in-memory)
  "locked_by":   str | None,   # worker id while leased, "__parked__" while parked, None when free
  "lock_expiry": float,        # epoch when the lease/park ends (0.0 when free)
}
```

`locked_by` + `lock_expiry` together are the **lease**: a message is *in flight* while
`locked_by is not None and lock_expiry >= now`. Unlike the durable backends the `event` is kept
as the live `Event` object (no JSON round-trip), matching the in-memory store's identity contract.

## Single-active-consumer-per-group

`claim` enforces the one invariant the whole design rests on — at most one in-flight message per
group — in two steps under the lock:

```text
in_flight = { m.group_id for m in messages if m.locked_by is not None and m.lock_expiry >= now }
for m in messages sorted by seq:                       # oldest first
    available = m.locked_by is None or m.lock_expiry < now
    if available and m.group_id not in in_flight:       # group has nothing in flight
        m.locked_by   = worker_id
        m.lock_expiry = now + visibility                # take the lease
        return Lease(m.seq, m.group_id, m.event)
return None
```

It first computes the set of groups that currently have a live lock, then leases the **oldest
free message whose group is not in that set**. So two messages of the same group are never both
in flight, and different groups are claimed independently. The `lock_expiry < now` test is the
**crash recovery**: a lease whose deadline has passed (a worker that died holding it) is treated
as free again — no separate sweeper.

## Operations

```text
publish(group_id, event)   # seq += 1; append {seq, group_id, event, locked_by=None, lock_expiry=0.0}
claim(worker_id, visibility)  # the select-then-lease above; None if nothing deliverable now
ack(lease)                 # drop the message: messages = [m for m in messages if m.seq != lease.seq]
nack(lease, delay=0)       # delay>0 -> park: locked_by="__parked__", lock_expiry=now+delay
                           # delay==0 -> retry now: locked_by=None, lock_expiry=0.0
close()                    # no-op (the list lives with the process)
```

`ack` removes the message (its group is then free for the next message). `nack(delay>0)` **parks**
it: `locked_by` is set to the `_PARKED` sentinel (non-null, so the `in_flight` check keeps the
group blocked) until `lock_expiry` passes — this is what lets a worker bounce a *suspended*
group's message without spinning on it (see the [control plane](../control-plane)). `nack(0)`
frees it for immediate retry.

## FIFO

Messages of a group are delivered oldest-first (`sorted by seq`), and a group is only ever
advanced by one consumer at a time, so within a group order is preserved.

See the [transports hub](../transports) for the contract and [distribution](../distribution) for
running workers.
