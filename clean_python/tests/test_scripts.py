# -*- coding: utf-8 -*-
"""Tests for script.py"""
from clean_python import scripts


def test_get_parser():
    parser = scripts.get_parser()
    # As a test, we just check one option. That's enough.
    options = parser.parse_args()
    assert options.verbose is False
