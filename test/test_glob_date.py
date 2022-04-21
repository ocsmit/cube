import pytest
from cubed.date_glob import DateGlob

date_str = "%Y-%m-%d"
correct_pattern = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"


def test_glob_date():
    pattern = DateGlob(date_str)
    assert pattern.pattern == correct_pattern
