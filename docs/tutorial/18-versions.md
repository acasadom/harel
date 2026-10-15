# 18. Changing a machine: versions

An execution can live for days — an order waiting for its approval, a subscription waiting for
its renewal. Sooner or later the machine it runs changes, while executions are still parked in
it. harel's answer: a changed machine is a **new version**, and every execution keeps running the
version it started on, to the end. New executions get the new one.

## Declaring a version

A machine declares its version after its name — a positive integer:

```text
machine order version 2 {
  ...
}
```

A machine without `version` is version 1. That is all the DSL there is to it.

## A machine with executions in flight

Here is version 1 of an order: it is submitted, reviewed and approved.

```python
import tempfile
from pathlib import Path

from harel import DictStore, DurableRunner, Event
from harel.published import publish_definitions, read_definitions
from harel.worker import load_definitions

V1 = """
event Submit {}
event Approve {}
machine order {
  initial Draft
  state Draft {}
  state Review {}
  final Done success
  from Draft to Review on Submit
  from Review to Done on Approve
}
"""

definitions_dir = Path(tempfile.mkdtemp())
(definitions_dir / "order.stm").write_text(V1)

store = DictStore()
publish_definitions(store, read_definitions(definitions_dir))  # what a worker does when it starts
runner = DurableRunner(store, load_definitions(definitions_dir))

waiting = runner.create("order")
waiting = runner.process(waiting.id, Event(kind="Submit"))
print(waiting.active_path, waiting.definition_version, waiting.definition_digest)
```

```text
Review 1 dfa90f99e9f82d62
```

Every execution records two things about its machine: the **version** it was created with, and
the **digest** of that version's source — a hash of the machine's text, its file's events,
guards, bindings and fragments, and the files it imports. Comments and layout don't count.

**Publishing** puts the version in the store, with its source. A worker does it with its
definitions directory when it starts; `harel publish DIR` does it from a deploy pipeline. Here we
call `publish_definitions` ourselves.

## Version 2

The business wants a shipping step between the approval and the end. Edit the file and bump the
version — the old file needn't be kept anywhere:

```python
V2 = """
event Submit {}
event Approve {}
event Shipped {}
machine order version 2 {
  initial Draft
  state Draft {}
  state Review {}
  state Shipping {}
  final Done success
  from Draft to Review on Submit
  from Review to Shipping on Approve
  from Shipping to Done on Shipped
}
"""
(definitions_dir / "order.stm").write_text(V2)

# the deploy: the new process publishes and loads only version 2
publish_definitions(store, read_definitions(definitions_dir))
runner = DurableRunner(store, load_definitions(definitions_dir))
```

The execution waiting in `Review` started on version 1. This runner has only version 2's file, so
it reads version 1 from the store — and the approval ends it, as version 1 says:

```python
waiting = runner.process(waiting.id, Event(kind="Approve"))
print(waiting.status.value, waiting.definition_version)
```

```text
DONE 1
```

A new execution gets the highest version the runner has loaded:

```python
fresh = runner.create("order")
fresh = runner.process(fresh.id, Event(kind="Submit"))
fresh = runner.process(fresh.id, Event(kind="Approve"))
print(fresh.active_path, fresh.definition_version)
```

```text
Shipping 2
```

`create("order", version=1)` asks for another loaded version — for a test, say.

## Changing a machine without a new version

Publishing a version stores it once. Change version 2 again without bumping it, and the publish
is refused — a worker starting with that file refuses to start, so the deploy stops before any
execution meets the change:

```python
from harel.published import DefinitionConflict

(definitions_dir / "order.stm").write_text(V2.replace("from Shipping to Done", "from Shipping to Review"))
try:
    publish_definitions(store, read_definitions(definitions_dir))
except DefinitionConflict as e:
    print(e)
```

```text
machine 'order' version 2 is published with other content (digest 3e9d1e5c5b97fe36, here 9099ddf2ea559ced): give the changed machine a new version
```

`harel hash order.stm` prints a machine's version and digest, so you can see a change coming
before a deploy.

## What the store doesn't keep: the actions

The store keeps the machines, not the Python code of their actions. A published version that says
`on enter charge` runs whatever `charge` is today, for as long as executions run on it — keep the
action working, with the same meaning, while they do.

If your code binds handlers and guards itself — `definition_from_dsl(..., actions=, guards=)`
rather than a `bind` in the DSL — give the runner the same bindings, so it can bind them in the
versions it reads from the store:

```text
DurableRunner(store, definitions, actions={"charge": charge}, guards={...})
```

A version read from the store may only call the action modules the runner's own machines call —
plus the ones `actions=` names, and any you allow with `action_modules=` or
`HAREL_ACTION_MODULES`. Someone able to write to the store can't make a worker run other code.

Next: [Changing a machine with executions in flight](../guide/durability.md#changing-a-machine-with-executions-in-flight)
covers every check in detail, and [the CLI](../guide/cli.md#versions-hash-and-publish) the two
commands.
