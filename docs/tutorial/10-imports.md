# 10. Splitting across files: imports

Shared events, guards, fragments, and whole machines belong in their own files so several
machines can reuse them. `import` pulls another `.stm` file in.

## A shared library file

Put the common vocabulary — here an event and a named guard — in `lib.stm`, and `import` it
from the order:

```text
# lib.stm
event Result { status: string }
guard ok = status == "ok"
```

```text
# order.stm
import "lib.stm"

machine order {
  initial Charging
  state Charging {}
  final Paid success {}
  final Failed failed {}

  from Charging to Paid   on Result where ok
  from Charging to Failed on Result where status == "no"
}
```

## Load it

`definition_from_dsl_file` reads a file and resolves its imports relative to that file's
directory. (The example writes the two files to a temporary directory so it runs as-is; in a
real project they'd just be files in your source tree.)

```python
import tempfile
from pathlib import Path

from harel import definition_from_dsl_file, DurableRunner, DictStore, Event

project = Path(tempfile.mkdtemp())
(project / "lib.stm").write_text("""
event Result { status: string }
guard ok = status == "ok"
""")
(project / "order.stm").write_text("""
import "lib.stm"

machine order {
  initial Charging
  state Charging {}
  final Paid success {}
  final Failed failed {}

  from Charging to Paid   on Result where ok
  from Charging to Failed on Result where status == "no"
}
""")

defn = definition_from_dsl_file(project / "order.stm", "order")
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)
exe = runner.process(exe.id, Event(kind="Result", data={"status": "ok"}))
print(exe.active_path, "/", exe.outcome)
```

```text
Paid / success
```

The `ok` guard defined in `lib.stm` is used in `order.stm` exactly as if it were local.

## What an import brings in, and how it's named

```{warning}
There is an asymmetry worth knowing. **Machines and fragments** are brought in **namespaced by
the import alias** (`import "x.stm" as ns` → `ns.Frag`, `ns.machineName`). But **events,
guards, and bindings** are imported **bare** — without a prefix. If an imported file and the
importing file both declare an event or guard with the same name, they silently collide and
the local one wins. Keep shared event/guard names distinct across files.
```

Aliased imports matter most for the next feature: an imported *machine* becomes a resolvable
target you can run as a black box. [Submachine invoke](11-invoke) is next.
