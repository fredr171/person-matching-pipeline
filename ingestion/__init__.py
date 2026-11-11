"""CDC events -> current-state table (build phase 3).

Validates, deduplicates, orders and applies events. Bound by the data
isolation rule (DESIGN.md section 3.1). See DESIGN.md section 4.
"""
