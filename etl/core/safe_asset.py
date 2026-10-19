# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Exception boundary for assets that handle person-identifying source data."""

from functools import wraps

import dagster as dg


def aggregate_errors(operation):
    """Expose a fixed failure description plus the exception class, never its text.

    Driver messages and chained values can carry source values; the class name
    cannot, and it is what an operator needs to tell a lock timeout from a
    missing table. Raise outside the except block so Dagster cannot serialize
    the original exception context. The function's signature is preserved for
    asset discovery.
    """

    @wraps(operation)
    def guarded(*args, **kwargs):
        kind = None
        try:
            return operation(*args, **kwargs)
        except Exception as exc:
            kind = type(exc).__name__
        raise dg.Failure(f"Operation failed ({kind}); no source values are exposed")

    return guarded
