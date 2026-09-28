import pytest

from matching.normalize import normalize_address, normalize_name, zip5


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Acme Supply, Inc.", "ACME SUPPLY INC"),
        ("ACME SUPPLY INCORPORATED", "ACME SUPPLY INC"),
        ("Smith & Sons Construction Company", "SMITH & SONS CONSTRUCTION CO"),
        ("Smith and Sons Construction Co.", "SMITH & SONS CONSTRUCTION CO"),
        ("Blue River L.L.C.", "BLUE RIVER LLC"),
        ("Blue  River,   LLC", "BLUE RIVER LLC"),
        ("Northwind Corporation", "NORTHWIND CORP"),
        ("Contoso Limited", "CONTOSO LTD"),
        (None, ""),
    ],
)
def test_normalize_name(raw, expected):
    assert normalize_name(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("123 North Main Street, Suite 400", "123 N MAIN ST STE 400"),
        ("123 N. Main St. Ste 400", "123 N MAIN ST STE 400"),
        ("P.O. Box 55", "PO BOX 55"),
        ("Post Office Box 55", "PO BOX 55"),
        ("", ""),
    ],
)
def test_normalize_address(raw, expected):
    assert normalize_address(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("20240-0001", "20240"),
        ("202400001", "20240"),
        ("08201", "08201"),
        (" 2024", ""),
        (None, ""),
    ],
)
def test_zip5(raw, expected):
    assert zip5(raw) == expected
