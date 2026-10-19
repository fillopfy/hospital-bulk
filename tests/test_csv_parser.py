from app.csv_parser import parse_csv


def test_valid_csv_with_optional_phone():
    r = parse_csv(b"name,address,phone\nA,1 St,555\nB,2 St,\n")
    assert r.ok
    assert [(x.row, x.name, x.phone) for x in r.rows] == [(1, "A", "555"), (2, "B", None)]


def test_phone_column_can_be_omitted_and_header_is_case_insensitive():
    r = parse_csv(b" Name , ADDRESS\nA,1 St\n")
    assert r.ok and r.rows[0].phone is None


def test_excel_bom_is_handled():
    assert parse_csv("﻿name,address\nA,1 St\n".encode("utf-8")).ok


def test_quoted_commas_in_address():
    r = parse_csv(b'name,address\nA,"12 Main St, Suite 4"\n')
    assert r.rows[0].address == "12 Main St, Suite 4"


def test_missing_required_column():
    r = parse_csv(b"name,phone\nA,555\n")
    assert not r.ok and "address" in r.errors[0].message


def test_row_level_errors_report_row_numbers():
    r = parse_csv(b"name,address\nA,1 St\n,2 St\nC,\n")
    assert [e.row for e in r.errors] == [2, 3]


def test_blank_lines_skipped():
    r = parse_csv(b"name,address\nA,1 St\n\n   \nB,2 St\n")
    assert r.ok and len(r.rows) == 2


def test_more_than_max_rows_rejected():
    body = "name,address\n" + "".join(f"H{i},Addr {i}\n" for i in range(21))
    r = parse_csv(body.encode(), max_rows=20)
    assert not r.ok and "21" in r.errors[-1].message


def test_empty_and_header_only():
    assert not parse_csv(b"").ok
    assert not parse_csv(b"name,address\n").ok


def test_non_utf8_rejected():
    assert not parse_csv("name,address\nCafé,1 St\n".encode("latin-1")).ok


def test_unknown_columns_warn_not_fail():
    r = parse_csv(b"name,address,email\nA,1 St,a@b.c\n")
    assert r.ok and r.warnings
