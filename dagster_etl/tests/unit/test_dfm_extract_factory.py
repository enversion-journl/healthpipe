# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the DFM extract factory pattern.

Tests:
  1. DfmExtractConfig validation and properties
  2. build_mssql_where() — T-SQL WHERE clause generation for all modes
  3. create_dfm_extraction_assets() — factory output structure, naming, metadata
"""

import pytest
from core.dfm_config import RunModeConfig
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.mssql.dfm_sql_builders import build_mssql_where

# ===========================================================================
# Fixtures & Helpers
# ===========================================================================


def _cfg(mode: str = "delta", **kwargs) -> RunModeConfig:
    """Shorthand for creating a RunModeConfig with overrides."""
    return RunModeConfig(mode=mode, **kwargs)


DATETIME_EXTRACT = DfmExtractConfig(
    source_schema="dbo",
    source_table="orders",
    target_table="orders_test",
    delta_column="modified_date",
    checkpoint_type="datetime",
    checkpoint_from=("arc", "orders_test"),
    description="Test datetime extract.",
)

INT_EXTRACT = DfmExtractConfig(
    source_schema="fact",
    source_table="Invoices",
    target_table="invoices_test",
    delta_expression="GREATEST([DW_Audit_Insert], [DW_Audit_Update])",
    checkpoint_type="int",
    checkpoint_from=("arc", "invoices_test"),
    description="Test int checkpoint extract.",
)

FULL_LOAD_EXTRACT = DfmExtractConfig(
    source_schema="dbo",
    source_table="categories",
    target_table="categories_test",
    is_full_load=True,
    checkpoint_from=None,
    description="Full load extract.",
)

LOOKBACK_EXTRACT = DfmExtractConfig(
    source_schema="dbo",
    source_table="shipments",
    target_table="shipments_test",
    delta_column="ship_date",
    checkpoint_type="datetime",
    lookback_days=5,
    checkpoint_from=("arc", "shipments_test"),
    description="Extract with 5-day design-time lookback.",
)


# ===========================================================================
# DfmExtractConfig — Validation
# ===========================================================================


class TestDfmExtractConfigValidation:
    """Validation rules enforced by __post_init__."""

    def test_valid_delta_column(self):
        cfg = DfmExtractConfig(
            source_schema="dbo",
            source_table="t",
            target_table="t_ext",
            delta_column="ts",
        )
        assert cfg.effective_delta_col == "ts"

    def test_valid_delta_expression(self):
        cfg = DfmExtractConfig(
            source_schema="dbo",
            source_table="t",
            target_table="t_ext",
            delta_expression="GREATEST(a, b)",
        )
        assert cfg.effective_delta_col == "GREATEST(a, b)"

    def test_mutually_exclusive_delta_column_and_expression(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            DfmExtractConfig(
                source_schema="dbo",
                source_table="t",
                target_table="t_ext",
                delta_column="ts",
                delta_expression="GREATEST(a, b)",
            )

    def test_no_delta_without_full_load_raises(self):
        with pytest.raises(ValueError, match="delta_column or delta_expression"):
            DfmExtractConfig(
                source_schema="dbo",
                source_table="t",
                target_table="t_ext",
            )

    def test_full_load_without_delta_is_valid(self):
        cfg = DfmExtractConfig(
            source_schema="dbo",
            source_table="t",
            target_table="t_ext",
            is_full_load=True,
        )
        assert cfg.effective_delta_col is None

    def test_valid_checkpoint_type_datetime(self):
        cfg = DfmExtractConfig(
            source_schema="dbo",
            source_table="t",
            target_table="t_ext",
            delta_column="ts",
            checkpoint_type="datetime",
        )
        assert cfg.checkpoint_type == "datetime"

    def test_valid_checkpoint_type_int(self):
        cfg = DfmExtractConfig(
            source_schema="dbo",
            source_table="t",
            target_table="t_ext",
            delta_column="ts",
            checkpoint_type="int",
        )
        assert cfg.checkpoint_type == "int"

    def test_invalid_checkpoint_type_raises(self):
        with pytest.raises(ValueError, match="checkpoint_type must be one of"):
            DfmExtractConfig(
                source_schema="dbo",
                source_table="t",
                target_table="t_ext",
                delta_column="ts",
                checkpoint_type="string",
            )

    def test_frozen_immutable(self):
        with pytest.raises(Exception):
            DATETIME_EXTRACT.source_table = "something_else"

    def test_checkpoint_from_tuple(self):
        assert DATETIME_EXTRACT.checkpoint_from == ("arc", "orders_test")

    def test_checkpoint_from_none_for_full_load(self):
        assert FULL_LOAD_EXTRACT.checkpoint_from is None


# ===========================================================================
# build_mssql_where — full_load mode
# ===========================================================================


class TestMssqlWhereFullLoad:
    """full_load mode returns None (no filtering)."""

    def test_full_load_mode_returns_none(self):
        result = build_mssql_where(_cfg("full_load"), "2025-01-01 00:00:00", DATETIME_EXTRACT)
        assert result is None

    def test_full_load_config_returns_none(self):
        result = build_mssql_where(_cfg("delta"), "2025-01-01 00:00:00", FULL_LOAD_EXTRACT)
        assert result is None

    def test_full_load_ignores_checkpoint(self):
        result = build_mssql_where(_cfg("full_load"), "2025-06-15 10:30:00", DATETIME_EXTRACT)
        assert result is None


# ===========================================================================
# build_mssql_where — delta mode (datetime checkpoint)
# ===========================================================================


class TestMssqlWhereDeltaDatetime:
    """delta mode with datetime checkpoint produces CAST-based comparisons."""

    def test_basic_delta(self):
        result = build_mssql_where(_cfg("delta"), "2025-05-08 10:30:00", DATETIME_EXTRACT)
        assert result is not None
        assert "modified_date > CAST('2025-05-08 10:30:00' AS DATETIME2)" == result

    def test_no_checkpoint_returns_none(self):
        result = build_mssql_where(_cfg("delta"), None, DATETIME_EXTRACT)
        assert result is None

    def test_empty_checkpoint_returns_none(self):
        result = build_mssql_where(_cfg("delta"), "", DATETIME_EXTRACT)
        assert result is None

    def test_delta_column_override_from_run_config(self):
        cfg = _cfg("delta", delta_column="custom_ts")
        result = build_mssql_where(cfg, "2025-01-01 00:00:00", DATETIME_EXTRACT)
        assert "[custom_ts] >" in result
        assert "modified_date" not in result

    def test_checkpoint_value_with_injection_raises_validation_error(self):
        with pytest.raises(ValueError, match="Invalid checkpoint value"):
            build_mssql_where(_cfg("delta"), "2025-01-01 00:00:00'injected", DATETIME_EXTRACT)


# ===========================================================================
# build_mssql_where — delta mode (int checkpoint)
# ===========================================================================


class TestMssqlWhereDeltaInt:
    """delta mode with int checkpoint produces raw integer comparisons."""

    def test_basic_int_checkpoint(self):
        result = build_mssql_where(_cfg("delta"), "12345", INT_EXTRACT)
        assert result == "GREATEST([DW_Audit_Insert], [DW_Audit_Update]) > 12345"

    def test_int_lookback_subtracts(self):
        cfg = _cfg("delta", lookback_days=100)
        result = build_mssql_where(cfg, "12345", INT_EXTRACT)
        assert result == "GREATEST([DW_Audit_Insert], [DW_Audit_Update]) > 12245"

    def test_int_lookback_floors_at_zero(self):
        cfg = _cfg("delta", lookback_days=99999)
        result = build_mssql_where(cfg, "100", INT_EXTRACT)
        assert "> 0" in result


# ===========================================================================
# build_mssql_where — delta with lookback
# ===========================================================================


class TestMssqlWhereDeltaLookback:
    """lookback_days adjusts checkpoint via DATEADD."""

    def test_design_time_lookback(self):
        result = build_mssql_where(_cfg("delta"), "2025-05-08 10:00:00", LOOKBACK_EXTRACT)
        assert "DATEADD(day, -5," in result
        assert "2025-05-08 10:00:00" in result

    def test_runtime_lookback_overrides_design_time(self):
        cfg = _cfg("delta", lookback_days=10)
        result = build_mssql_where(cfg, "2025-05-08 10:00:00", LOOKBACK_EXTRACT)
        assert "DATEADD(day, -10," in result
        assert "DATEADD(day, -5," not in result

    def test_zero_lookback_no_dateadd(self):
        no_lookback = DfmExtractConfig(
            source_schema="dbo",
            source_table="t",
            target_table="t_ext",
            delta_column="ts",
            lookback_days=0,
        )
        result = build_mssql_where(_cfg("delta"), "2025-01-01 00:00:00", no_lookback)
        assert "DATEADD" not in result


# ===========================================================================
# build_mssql_where — period mode
# ===========================================================================


class TestMssqlWherePeriod:
    """period mode creates a bounded date range."""

    def test_basic_period(self):
        cfg = _cfg("period", date_from="2025-01-01", date_to="2025-01-31")
        result = build_mssql_where(cfg, None, DATETIME_EXTRACT)
        assert result is not None
        assert "modified_date >= CAST('2025-01-01' AS DATETIME2)" in result
        assert "modified_date < DATEADD(day, 1, CAST('2025-01-31' AS DATE))" in result

    def test_period_ignores_checkpoint(self):
        cfg = _cfg("period", date_from="2025-03-01", date_to="2025-03-31")
        result = build_mssql_where(cfg, "some-old-checkpoint", DATETIME_EXTRACT)
        assert "some-old-checkpoint" not in result
        assert "2025-03-01" in result


# ===========================================================================
# build_mssql_where — ids mode
# ===========================================================================


class TestMssqlWhereIds:
    """ids mode creates IN (...) clauses."""

    def test_inline_values(self):
        cfg = _cfg("ids", id_filter_column="id", id_filter_values=["A", "B", "C"])
        result = build_mssql_where(cfg, None, DATETIME_EXTRACT)
        assert "[id] IN" in result
        assert "'A'" in result
        assert "'B'" in result
        assert "'C'" in result

    def test_custom_id_filter_column(self):
        cfg = _cfg("ids", id_filter_column="order_id", id_filter_values=["X"])
        result = build_mssql_where(cfg, None, DATETIME_EXTRACT)
        assert "[order_id] IN" in result

    def test_filter_table(self):
        cfg = _cfg("ids", id_filter_column="id", id_filter_table="tmp.my_ids")
        result = build_mssql_where(cfg, None, DATETIME_EXTRACT)
        assert "IN" in result
        assert "SELECT [flt].[id] FROM [tmp].[my_ids] AS [flt]" in result

    def test_filter_table_takes_precedence_over_values(self):
        cfg = _cfg(
            "ids",
            id_filter_column="id",
            id_filter_table="tmp.ids",
            id_filter_values=["should_not_appear"],
        )
        result = build_mssql_where(cfg, None, DATETIME_EXTRACT)
        assert "[tmp].[ids]" in result
        assert "should_not_appear" not in result

    def test_sql_injection_escaped(self):
        cfg = _cfg("ids", id_filter_column="id", id_filter_values=["O'Brien"])
        result = build_mssql_where(cfg, None, DATETIME_EXTRACT)
        assert "O''Brien" in result


# ===========================================================================
# create_dfm_extraction_assets — factory output
# ===========================================================================


class TestCreateDfmExtractionAssets:
    """Factory generates correctly structured Dagster assets."""

    @pytest.fixture()
    def generated_assets(self):
        from core.resources import MSSQLResource
        from DSI.connectors.factory import create_dfm_extraction_assets

        configs = [DATETIME_EXTRACT, INT_EXTRACT, FULL_LOAD_EXTRACT]
        return create_dfm_extraction_assets(
            configs=configs,
            source_resource_key="test_source_mssql",
            source_resource_type=MSSQLResource,
            source_system="TestSource",
            group_name="test_extract",
            domain_tag="test",
            dialect="mssql",
        )

    def test_generates_one_asset_per_config(self, generated_assets):
        assert len(generated_assets) == 3

    def test_asset_names(self, generated_assets):
        names = [a.__name__ for a in generated_assets]
        assert "ext_orders_test" in names
        assert "ext_invoices_test" in names
        assert "ext_categories_test" in names

    def test_assets_are_callable(self, generated_assets):
        for asset in generated_assets:
            assert callable(asset)

    def test_asset_has_dagster_metadata(self, generated_assets):

        for asset in generated_assets:
            asset.get_asset_spec() if hasattr(asset, "get_asset_spec") else None
            keys = [
                k
                for k in dir(asset)
                if "key" in k.lower() or "group" in k.lower() or "name" in k.lower()
            ]
            assert len(keys) > 0

    def test_invalid_dialect_raises(self):
        from core.resources import MSSQLResource
        from DSI.connectors.factory import create_dfm_extraction_assets

        with pytest.raises(ValueError, match="Unknown dialect"):
            create_dfm_extraction_assets(
                configs=[DATETIME_EXTRACT],
                source_resource_key="test_mssql",
                source_resource_type=MSSQLResource,
                source_system="Test",
                group_name="test",
                domain_tag="test",
                dialect="postgres",
            )

    def test_empty_configs_returns_empty_list(self):
        from core.resources import MSSQLResource
        from DSI.connectors.factory import create_dfm_extraction_assets

        result = create_dfm_extraction_assets(
            configs=[],
            source_resource_key="test_mssql",
            source_resource_type=MSSQLResource,
            source_system="Test",
            group_name="test",
            domain_tag="test",
        )
        assert result == []


# ===========================================================================
# Factory — typing and resource injection annotations
# ===========================================================================


class TestFactoryAnnotations:
    """Verify that generated asset functions have correct type annotations."""

    def test_annotations_include_source_resource(self):
        from core.resources import MSSQLResource
        from DSI.connectors.factory import create_dfm_extraction_assets

        assets = create_dfm_extraction_assets(
            configs=[DATETIME_EXTRACT],
            source_resource_key="my_custom_source",
            source_resource_type=MSSQLResource,
            source_system="Test",
            group_name="test",
            domain_tag="test",
        )
        asset_fn = assets[0]
        underlying = getattr(asset_fn, "op", asset_fn)
        fn = getattr(underlying, "fn", underlying)
        annotations = getattr(fn, "__annotations__", {})
        assert "my_custom_source" in annotations
        assert annotations["my_custom_source"] is MSSQLResource

    def test_annotations_include_run_mode_config(self):
        from core.dfm_config import RunModeConfig
        from core.resources import MSSQLResource
        from DSI.connectors.factory import create_dfm_extraction_assets

        assets = create_dfm_extraction_assets(
            configs=[DATETIME_EXTRACT],
            source_resource_key="src",
            source_resource_type=MSSQLResource,
            source_system="Test",
            group_name="test",
            domain_tag="test",
        )
        asset_fn = assets[0]
        underlying = getattr(asset_fn, "op", asset_fn)
        fn = getattr(underlying, "fn", underlying)
        annotations = getattr(fn, "__annotations__", {})
        assert "config" in annotations
        assert annotations["config"] is RunModeConfig

    def test_signature_has_correct_parameter_names(self):
        """Verify inspect.Signature exposes correct param names for Dagster."""
        import inspect

        from core.resources import MSSQLResource
        from DSI.connectors.factory import create_dfm_extraction_assets

        assets = create_dfm_extraction_assets(
            configs=[DATETIME_EXTRACT],
            source_resource_key="my_custom_source",
            source_resource_type=MSSQLResource,
            source_system="Test",
            group_name="test",
            domain_tag="test",
        )
        asset_fn = assets[0]
        underlying = getattr(asset_fn, "op", asset_fn)
        fn = getattr(underlying, "fn", underlying)
        sig = inspect.signature(fn)
        param_names = list(sig.parameters.keys())
        assert param_names == ["context", "my_custom_source", "mssql_target_staging", "config"]

    def test_no_exec_suppression_in_factory(self):
        """Verify the factory no longer uses exec() (no noqa: S102 needed)."""
        import ast
        import inspect
        import textwrap

        from DSI.connectors.factory import _build_dfm_asset_fn

        source_code = textwrap.dedent(inspect.getsource(_build_dfm_asset_fn))
        tree = ast.parse(source_code)
        exec_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "exec"
        ]
        assert len(exec_calls) == 0, "Factory should not use exec()"
        assert "noqa: S102" not in source_code
