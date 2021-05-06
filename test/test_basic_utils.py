import pytest

from util.basic_utils import get_basic_utilities
# noinspection PyUnresolvedReferences
from .confest import cleanup


@pytest.fixture
def basic_utilities():
    return get_basic_utilities()


def test_cleanup(cleanup):
    assert True


def test_get_basic_utilities(basic_utilities):
    get_basic_utilities()
    assert True


def test_get_basic_utilities_singleton(basic_utilities):
    new_basic_utilities = get_basic_utilities()
    assert new_basic_utilities is basic_utilities
