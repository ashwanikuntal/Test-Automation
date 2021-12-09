from main import count_csv_file_rows


def test_count_csv_file():
    assert count_csv_file_rows() == 608
