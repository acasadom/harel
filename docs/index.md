# harel

*Durable, distributed statecharts for Python.*

A hierarchical **statechart** engine for Python — with the durability and distribution of a
workflow engine, but the right model underneath.

You author a machine in a small textual DSL (`.stm`), it compiles to an immutable
`Definition`, and a pure, effects-based engine runs it against a serializable `Execution`
that can be persisted, distributed across workers, and survive crashes.

This documentation is a **step-by-step tutorial**: each page introduces one capability and
builds on the last, using machines you already know (a turnstile, an online order, a
microwave, a keyboard). Every Python block on every page is executed in CI, so you can copy,
paste, and run it as-is.

## The tutorial, step by step

Each step introduces one capability and builds on the last, growing a single example — an
online order — with side trips to a turnstile, a checkout, and a parcel shipment where they
illustrate a feature better.

```{toctree}
:maxdepth: 1
:caption: Tutorial

tutorial/01-getting-started
tutorial/02-actions
tutorial/03-outcomes
tutorial/04-guards
tutorial/05-selectors
tutorial/06-hierarchy
tutorial/07-timers
tutorial/08-orthogonal
tutorial/09-fragments
tutorial/10-imports
tutorial/11-invoke
tutorial/12-fanout
tutorial/13-payloads
tutorial/14-validation
```

## Operations & reference

How these machines run for real — and the reference material.

```{toctree}
:maxdepth: 1
:caption: Operations & reference

guide/cli
guide/visualization
guide/durability
guide/distribution
guide/control-plane
guide/faas
guide/dsl-reference
guide/api-reference
```

## Is a statechart the right tool?

Use a statechart when your domain **is** a machine of named states with hierarchy, guarded
transitions, and explicit terminal verdicts — an order, a subscription, a device, a claim, a
deployment. These are routinely mislabelled "pipelines" or "workflows" and built with a DAG
runner or a pile of `if status == …` branches, which is where the illegal states and the
hard-to-follow retry/cancel logic come from. If your domain is genuinely "run these steps in
order with retries", a workflow engine is the better fit — harel is not trying to replace
one.
