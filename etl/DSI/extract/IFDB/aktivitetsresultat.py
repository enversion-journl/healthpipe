# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Column contract for IFDB.AKTIVITETSRESULTAT.

Extraction itself runs through the DFM archive layer (Oracle -> ext.* -> arc.*,
see ``DSI/oracle/ifdb_extract.py``). What remains here is the
``TableExtractionConfig``, which the Oracle schema-verification integration
tests assert against — the partitioned extract assets this module used to
generate were never registered in ``definitions.py`` and are gone.
"""

from core.extract import ExtractBatchingPolicy
from DSI.connectors.oracle.models import TableExtractionConfig

AKTIVITETSRESULTAT_CONFIG = TableExtractionConfig(
    table="IFDB.AKTIVITETSRESULTAT",
    columns=[
        "OBJEKT_ID",
        "MODTIDSPUNKT",
        "SLETTET",
        "PATIENT_ID",
        "UDFOERELSE_ID",
        "AKTIVITETTYPE_KODE",
        "AKTIVITETTYPE_KODETEKST",
        "RESULTATTYPE_KODE",
        "RESULTATTYPE_KODETEKST",
        "OVERSKRIFT",
        "HIERARKISK_PLACERING",
        "DOMAENENAVN",
        "MODELLERINGSNAVN",
        "MODELLERINGSVERSION",
        "XML",
        "TILSTEDEVAERELSE_ID",
        "UDFOERT_PERSID",
        "UDFOERT_PERSNAVN",
        "UDFOERT_BRUGERROLLE",
        "UDFOERT_ADMENHED_KODE",
        "UDFOERT_ADMENHED_NAVN",
        "UDFOERT_ADMENHED_ORGKODE",
        "UDFOERT_TIDSPUNKT",
        "KLADDE",
        "OPDATERET_TID",
        "ENCOUNTER_ID",
        "EPISODE_OF_CARE_ID",
        "PERIOD_START",
        "PERIOD_END",
        "UDFOERT_ADMENHED_SORCODE",
        "LPR3_IGNORED",
        "UDFOERT_BRUGERROLLE_NAME",
        "RESULT_TYPE",
        "RESULT_MOID",
    ],
    partition_column="OPDATERET_TID",
    description=(
        "Clinical activity results from IFDB, including XML CLOB with structured form data."
    ),
    batching=ExtractBatchingPolicy(
        enabled=True,
        fetch_size=50_000,
        env_prefix="IFDB_EXTRACT_AKTIVITETSRESULTAT",
        write_batch_sidecars=True,
    ),
)

__all__ = ["AKTIVITETSRESULTAT_CONFIG"]
