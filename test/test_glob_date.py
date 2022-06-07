import pytest
from cubed.date_glob import DateGlob


def test_glob_date1():
    date_str = "%Y-%m-%d"
    correct_pattern = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"
    pattern = DateGlob(date_str)
    assert pattern.pattern == correct_pattern


def test_glob_date2():
    date_str = "%Y%m%d"
    correct_pattern = "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
    pattern = DateGlob(date_str)
    assert pattern.pattern == correct_pattern


def test_glob_date_fail():
    date_str = "%Y-%m-%d"
    correct_pattern = "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
    pattern = DateGlob(date_str)
    assert pattern.pattern != correct_pattern
