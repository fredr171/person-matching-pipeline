"""Shared plumbing for the person matching pipeline.

This package holds what every stage needs and no stage owns: repository paths,
typed loaders for the YAML configuration, typed loaders for the reference data
in ``config/reference/``, and the command-line entry point
(``python -m pipeline``).
"""
