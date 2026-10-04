# Monitor demo — example

> How it works — the machines, how it is built, and why it runs the way it does: [the
> monitor_demo page](https://acasadom.github.io/harel/examples/monitor_demo.html) of the docs.

Seeds a SQLite store with executions in every state the monitor shows — in progress, on a timer,
suspended, done, dead-lettered, an orthogonal join, an unknown definition — each with its trace,
then opens the monitor on it.

```bash
uv run python -m examples.monitor_demo.seed /tmp/harel-demo.db
HAREL_STORE_BACKEND=sqlite HAREL_STORE_DB=/tmp/harel-demo.db \
    uv run harel monitor --definitions-dir examples/monitor_demo/machines
```
