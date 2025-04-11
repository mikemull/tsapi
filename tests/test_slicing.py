import pytest
from tsapi.dataset_cache import DatasetCache


@pytest.fixture()
def dataset_cache():
    return DatasetCache


def test_get_new_slice_within_bounds(dataset_cache):
    assert dataset_cache.get_new_slice(10, 20, 15, 5) == (5, 5)


def test_get_new_slice_exact_bounds(dataset_cache):
    assert dataset_cache.get_new_slice(10, 20, 10, 20) == (0, 20)


def test_get_new_slice_start_boundary(dataset_cache):
    assert dataset_cache.get_new_slice(10, 20, 10, 10) == (0, 10)


def test_get_new_slice_end_boundary(dataset_cache):
    assert dataset_cache.get_new_slice(10, 20, 20, 10) == (10, 10)


def test_get_new_slice_out_of_bounds(dataset_cache):
    with pytest.raises(ValueError):
        dataset_cache.get_new_slice(10, 20, 5, 10)


def test_get_new_slice_exceeding_bounds(dataset_cache):
    with pytest.raises(ValueError):
        dataset_cache.get_new_slice(10, 20, 25, 10)


def test_get_new_slice_negative_offset(dataset_cache):
    with pytest.raises(ValueError):
        dataset_cache.get_new_slice(10, 20, -5, 10)
