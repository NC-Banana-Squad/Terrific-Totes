import pytest
import pandas as pd
from transform_utils import dim_counterparty

def test_counterparty_basic():
    df1 = pd.DataFrame(
        {
            "counterparty_id": [1],
            "counterparty_legal_name": ["Fahey & Sons"],
            "legal_address_id": [28],
            "commercial_contact": ["Jean Hane II"],
            "created_at": ["2022-11-03"],
            "last_updated": ["2022-11-03"],
        }
    )

    df2 = pd.DataFrame(
        {
            "address_id": [28],
            "address_line_1": ["179 Alexie Cliffs"],
            "address_line_2": [None],
            "district": ["Avon"],
            "city": ["Lake Charles"],
            "postal_code": ["28441"],
            "country": ["Turkey"],
            "phone": ["1803 637401"],
            "created_at": ["2022-11-03"],
            "last_updated": ["2022-11-03"],
        }
    )

    expected = pd.DataFrame(
        {
            "counterparty_id": [1],
            "counterparty_legal_name": ["Fahey & Sons"],
            "counterparty_legal_address_line_1": ["179 Alexie Cliffs"],
            "counterparty_legal_address_line_2": [None],
            "counterparty_legal_district": ["Avon"],
            "counterparty_legal_city": ["Lake Charles"],
            "counterparty_legal_postal_code": ["28441"],
            "counterparty_legal_country": ["Turkey"],
            "counterparty_legal_phone_number": ["1803 637401"],
        }
    )

    result = dim_counterparty(df1, df2)

    pd.testing.assert_frame_equal(result, expected)


def test_counterparty_no_match():
    df1 = pd.DataFrame(
        {
            "counterparty_id": [1],
            "counterparty_legal_name": ["Fahey & Sons"],
            "legal_address_id": [99],  # No match
            "commercial_contact": ["Jean Hane II"],
            "created_at": ["2022-11-03 14:20:51.563000"],
            "created_at": ["2022-11-03"],
            "last_updated": ["2022-11-03"],
        }
    )

    df2 = pd.DataFrame(
        {
            "address_id": [28],
            "address_line_1": ["179 Alexie Cliffs"],
            "address_line_2": [None],
            "district": ["Avon"],
            "city": ["Lake Charles"],
            "postal_code": ["28441"],
            "country": ["Turkey"],
            "phone": ["1803 637401"],
            "created_at": ["2022-11-03"],
            "last_updated": ["2022-11-03"],
        }
    )

    expected = pd.DataFrame(
        {
            "counterparty_id": pd.Series([], dtype="int64"),
            "counterparty_legal_name": pd.Series([], dtype="object"),
            "counterparty_legal_address_line_1": pd.Series([], dtype="object"),
            "counterparty_legal_address_line_2": pd.Series([], dtype="object"),
            "counterparty_legal_district": pd.Series([], dtype="object"),
            "counterparty_legal_city": pd.Series([], dtype="object"),
            "counterparty_legal_postal_code": pd.Series([], dtype="object"),
            "counterparty_legal_country": pd.Series([], dtype="object"),
            "counterparty_legal_phone_number": pd.Series([], dtype="object"),
        }
    )

    result = dim_counterparty(df1, df2)

    pd.testing.assert_frame_equal(result, expected)
