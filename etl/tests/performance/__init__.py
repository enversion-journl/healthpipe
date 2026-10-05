# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Performance tests for FHIR ETL pipelines.

These tests require:
1. Real database connections (USE_REAL_DB_FOR_TESTS=true)
2. Performance test data seeded in Oracle (run seed_ifdb_performance_data.py)
3. Significant runtime (marked with @pytest.mark.slow)

Run with:
    USE_REAL_DB_FOR_TESTS=true pytest tests/performance/ -v -s --tb=short
"""
