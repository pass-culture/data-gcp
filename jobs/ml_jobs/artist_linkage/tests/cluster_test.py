import pytest

from cluster import chunks


class TestCluster:
    class TestChunks:
        @staticmethod
        def test_chunks():
            # Test case where list size is a multiple of chunk size
            lst = [1, 2, 3, 4, 5, 6]
            chunk_size = 2
            expected_output = [[1, 2], [3, 4], [5, 6]]
            assert list(chunks(lst, chunk_size)) == expected_output

            # Test case where list size is not a multiple of chunk size
            lst = [1, 2, 3, 4, 5]
            chunk_size = 2
            expected_output = [[1, 2], [3, 4], [5]]
            assert list(chunks(lst, chunk_size)) == expected_output

            # Test case where chunk size is larger than list size
            lst = [1, 2, 3]
            chunk_size = 5
            expected_output = [[1, 2, 3]]
            assert list(chunks(lst, chunk_size)) == expected_output

            # Test case where list is empty
            lst = []
            chunk_size = 2
            expected_output = []
            assert list(chunks(lst, chunk_size)) == expected_output

        @staticmethod
        def test_chunks_with_non_list_input():
            with pytest.raises(TypeError):
                list(chunks(123, 2))

        @staticmethod
        def test_chunks_with_zero_chunk_size():
            with pytest.raises(ValueError):
                list(chunks([1, 2, 3], 0))

        @staticmethod
        def test_chunks_with_non_integer_chunk_size():
            with pytest.raises(TypeError):
                list(chunks([1, 2, 3], "2"))
