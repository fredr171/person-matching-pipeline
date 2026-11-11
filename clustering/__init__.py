"""Shared constrained clustering and stable ID reconciliation (build phase 5).

Called by both the dbt Python model and PySpark ``applyInPandas`` (D-006).
Bound by the data isolation rule (DESIGN.md section 3.1). See DESIGN.md sections 10 and 11.
"""
