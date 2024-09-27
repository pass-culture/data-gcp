import pytest

from app.retrieval.core.filter import Filter


@pytest.fixture
def filter_instance():
    return Filter()


def test_empty_tree_data(filter_instance):
    assert filter_instance.parse_where_clause() == ""


def test_simple_condition(filter_instance):
    filter_instance.tree_data = {"age": {"$gt": 30}}
    assert filter_instance.parse_where_clause() == "( age > 30 )"


def test_multiple_conditions_with_and(filter_instance):
    filter_instance.tree_data = {"age": {"$gt": 30}, "price": {"$lt": 5000}}
    expected_clause = "( age > 30 ) AND ( price < 5000 )"
    assert filter_instance.parse_where_clause() == expected_clause


def test_logical_or_operator(filter_instance):
    filter_instance.tree_data = {
        "$or": [{"age": {"$gt": 30}}, {"price": {"$lt": 5000}}]
    }
    expected_clause = "(( age > 30 ) OR ( price < 5000 ))"
    assert filter_instance.parse_where_clause() == expected_clause


def test_nested_logical_and_or(filter_instance):
    filter_instance.tree_data = {
        "$and": [
            {"$or": [{"age": {"$gt": 30}}, {"price": {"$lt": 5000}}]},
            {"name": {"$eq": "John"}},
        ]
    }
    expected_clause = "((( age > 30 ) OR ( price < 5000 )) AND ( name = John ))"
    assert filter_instance.parse_where_clause() == expected_clause


def test_membership_in_operator(filter_instance):
    filter_instance.tree_data = {"category": {"$in": ["Books", "Cinema"]}}
    expected_clause = "( category IN ( Books, Cinema ) )"
    assert filter_instance.parse_where_clause() == expected_clause


def test_membership_nin_operator(filter_instance):
    filter_instance.tree_data = {"category": {"$nin": ["Books", "Cinema"]}}
    expected_clause = "( category NOT IN ( Books, Cinema ) )"
    assert filter_instance.parse_where_clause() == expected_clause


def test_unsupported_operator(filter_instance):
    filter_instance.tree_data = {"price": {"$unknown": 100}}
    with pytest.raises(ValueError) as exc_info:
        filter_instance.parse_where_clause()
    assert "The operator $unknown is not supported" in str(exc_info.value)


def test_multiple_conditions_on_same_key(filter_instance):
    filter_instance.tree_data = {"age": {"$gt": 30, "$lt": 50}}
    expected_clause = "( age > 30 ) AND ( age < 50 )"
    assert filter_instance.parse_where_clause() == expected_clause


def test_non_list_value_in_in_operator(filter_instance):
    filter_instance.tree_data = {"category": {"$in": "Books"}}
    with pytest.raises(ValueError) as exc_info:
        filter_instance.parse_where_clause()
    assert "requires a list or tuple" in str(exc_info.value)
