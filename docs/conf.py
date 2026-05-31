"""Sphinx configuration for the harel documentation (Markdown via MyST)."""

project = "harel"
author = "Alberto Casado"

extensions = ["myst_parser", "sphinxcontrib.mermaid"]

# Documentation is authored in Markdown.
source_suffix = {".md": "markdown"}
root_doc = "index"

exclude_patterns = ["_build"]

html_theme = "alabaster"

# Resolve in-page links like [text](#a-heading) to heading anchors (h1..h3).
myst_heading_anchors = 3

# PlantUML code fences are kept as plain text (no diagram rendering, to avoid a
# Java/PlantUML build dependency); silence the "unknown lexer" highlight warning.
suppress_warnings = ["misc.highlighting_failure"]
