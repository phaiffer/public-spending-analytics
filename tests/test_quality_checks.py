from decimal import Decimal

import pandas as pd
import pytest

from gov_spending_analytics.quality.basic_checks import (
    check_non_negative_amount,
    check_required_columns,
    check_source_traceability,
    raise_for_quality_failures,
)


def test_required_columns_check_flags_missing_and_null_values() -> None:
    frame = pd.DataFrame({"amount_brl": [Decimal("1.00"), None]})

    failures = check_required_columns(frame, ["amount_brl", "source_file_name"])

    assert [failure.check_name for failure in failures] == [
        "required_columns_present",
        "required_columns_not_null",
    ]


def test_non_negative_amount_check_flags_negative_values() -> None:
    frame = pd.DataFrame({"amount_brl": [Decimal("1.00"), Decimal("-0.01")]})

    failures = check_non_negative_amount(frame)

    assert len(failures) == 1
    assert failures[0].check_name == "amount_non_negative"


def test_source_traceability_check_accepts_valid_traceability_fields() -> None:
    frame = pd.DataFrame(
        {
            "source_system": ["portal_transparencia"],
            "source_family": ["despesas"],
            "source_file_name": ["20250101_Despesas_Pagamento.csv"],
            "source_file_path": ["data/raw/portal_transparencia/despesas/file.csv"],
            "source_profile_name": ["20250101_Despesas_Pagamento_profile.json"],
            "source_row_number": [1],
            "spending_stage": ["payment"],
        }
    )

    failures = check_source_traceability(frame, frame.columns)

    assert failures == []


def test_raise_for_quality_failures_raises_compact_error() -> None:
    frame = pd.DataFrame({"amount_brl": [Decimal("-0.01")]})
    failures = check_non_negative_amount(frame)

    with pytest.raises(ValueError, match="amount_non_negative"):
        raise_for_quality_failures(failures)
