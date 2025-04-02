import pytest
from tsapi.model.dataset import get_new_slice


def test_get_new_slice_within_bounds():
    assert get_new_slice(10, 20, 15, 5) == (5, 5)


def test_get_new_slice_exact_bounds():
    assert get_new_slice(10, 20, 10, 20) == (0, 20)


def test_get_new_slice_start_boundary():
    assert get_new_slice(10, 20, 10, 10) == (0, 10)


def test_get_new_slice_end_boundary():
    assert get_new_slice(10, 20, 20, 10) == (10, 10)


def test_get_new_slice_out_of_bounds():
    with pytest.raises(ValueError):
        get_new_slice(10, 20, 5, 10)


def test_get_new_slice_exceeding_bounds():
    with pytest.raises(ValueError):
        get_new_slice(10, 20, 25, 10)


def test_get_new_slice_negative_offset():
    with pytest.raises(ValueError):
        get_new_slice(10, 20, -5, 10)
