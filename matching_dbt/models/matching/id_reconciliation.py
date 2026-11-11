"""Stable person ID reconciliation through the shared Python function (DESIGN.md section 11).

Returns one table with three kinds of rows, split by the ``person_assignment``,
``person_id_crosswalk`` and ``id_state`` models:

* ``row_type = 'assignment'``: record uid -> person id.
* ``row_type = 'retirement'``: a retired id, its successor (or null) and the reason.
* ``row_type = 'state'``: the next unused sequence number.
"""

import pandas as pd

from clustering.ids import reconcile
from pipeline.config import PersonIdFormat


def model(dbt, session):
    dbt.config(materialized="table")
    id_format = PersonIdFormat(
        prefix=dbt.config.get("person_id_prefix"),
        digits=int(dbt.config.get("person_id_digits")),
    )
    clusters = dbt.ref("clusters").project("record_key, cluster_id").fetchall()
    uids = dict(dbt.ref("int_records_standardized").project("record_key, record_uid").fetchall())
    previous = dict(dbt.ref("stg_previous_assignment").project("record_uid, person_id").fetchall())
    ((next_sequence,),) = dbt.ref("stg_previous_id_state").project("next_sequence").fetchall()

    result = reconcile(
        {uids[key]: cluster for key, cluster in clusters}, previous, int(next_sequence), id_format
    )

    rows = [
        {"row_type": "assignment", "record_uid": uid, "person_id": person_id}
        for uid, person_id in result.person_of.items()
    ]
    rows += [
        {
            "row_type": "retirement",
            "retired_id": r.retired_id,
            "successor_id": r.successor_id,
            "reason": r.reason,
        }
        for r in result.retirements
    ]
    rows.append({"row_type": "state", "next_sequence": result.next_sequence})
    columns = [
        "row_type",
        "record_uid",
        "person_id",
        "retired_id",
        "successor_id",
        "reason",
        "next_sequence",
    ]
    frame = pd.DataFrame(rows, columns=columns)
    frame["next_sequence"] = frame["next_sequence"].astype("Int64")
    return frame.astype({c: "string" for c in columns if c != "next_sequence"})
