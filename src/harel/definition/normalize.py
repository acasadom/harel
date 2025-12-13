"""Pure YAML/JSON normalization for state-machine definitions.

These helpers transform the raw parsed config (the format authored in YAML/JSON)
into a canonical dict before the domain objects are built. They are independent
of any (de)serialization library — plain Python over dicts/lists.

What they do:
- ``normalize_transitions``: flatten transition lists and expand a ``from`` list
  into one transition per source.
- ``normalize_states``: turn the name-keyed ``states`` mapping into entries that
  carry their ``name``, hierarchical ``full_path`` and resolved ``type``.
"""

import copy
import itertools
from typing import Dict

# Valid values for a state's "type" discriminator (kept here to avoid coupling
# normalization to the object/schema layer).
STATE_TYPE_NAMES = (
    "State",
    "CompositeState",
    "ParallelState",
    "OrthogonalState",
)


class NormalizeError(Exception):
    pass


def normalize_transitions(data: Dict, **kwargs) -> Dict:
    transitions = data.pop("transitions", []) or []

    if transitions and isinstance(transitions[0], (list, tuple)):
        transitions = itertools.chain.from_iterable(transitions)

    new_transitions = []

    for transition in transitions:
        source = transition.get("from", None)
        if isinstance(source, list):
            transition.pop("from")
            for s in source:
                t = copy.deepcopy(transition)
                t["from"] = s
                new_transitions.append(t)
        else:
            new_transitions.append(transition)

    if new_transitions:
        data["transitions"] = new_transitions

    return data


def normalize_states(data: Dict, **kwargs) -> Dict:
    states = data.get("states", None) or None
    if states is None:
        raise NormalizeError("No state has been defined...")

    def check(value):
        if value is not None:
            if value not in STATE_TYPE_NAMES:
                raise NormalizeError(f" State type '{value}' not a valid type ({STATE_TYPE_NAMES})")
            return True
        return False

    data["states"] = dict(
        (
            name,
            {
                **state,
                "name": name,
                "full_path": ".".join(filter(None, [data.get("full_path", None), name])),
                "type": next(
                    filter(
                        check, [state.get("type", None), ("CompositeState" if "states" in state else "State")]
                    )
                ),
            },
        )
        for name, state in states.items()
    )

    return data
