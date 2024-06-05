# import pytest
# from scipy.sparse import csr_matrix

# from cluster import chunks, compute_distance_matrix


# class TestCluster:
#     class TestChunks:
#         @staticmethod
#         def test_chunks():
#             # Test case where list size is a multiple of chunk size
#             lst = [1, 2, 3, 4, 5, 6]
#             chunk_size = 2
#             expected_output = [[1, 2], [3, 4], [5, 6]]
#             assert list(chunks(lst, chunk_size)) == expected_output

#             # Test case where list size is not a multiple of chunk size
#             lst = [1, 2, 3, 4, 5]
#             chunk_size = 2
#             expected_output = [[1, 2], [3, 4], [5]]
#             assert list(chunks(lst, chunk_size)) == expected_output

#             # Test case where chunk size is larger than list size
#             lst = [1, 2, 3]
#             chunk_size = 5
#             expected_output = [[1, 2, 3]]
#             assert list(chunks(lst, chunk_size)) == expected_output

#             # Test case where list is empty
#             lst = []
#             chunk_size = 2
#             expected_output = []
#             assert list(chunks(lst, chunk_size)) == expected_output

#         @staticmethod
#         def test_chunks_with_non_list_input():
#             with pytest.raises(TypeError):
#                 list(chunks(123, 2))

#         @staticmethod
#         def test_chunks_with_zero_chunk_size():
#             with pytest.raises(ValueError):
#                 list(chunks([1, 2, 3], 0))

#         @staticmethod
#         def test_chunks_with_non_integer_chunk_size():
#             with pytest.raises(TypeError):
#                 list(chunks([1, 2, 3], "2"))

#     class TestComputeDistanceMatrix:
#         @staticmethod
#         @pytest.mark.parametrize(
#             "artists_list, num_chunks, expected_shape",
#             [
#                 (["artist1", "artist2", "artist3"], 2, (3, 3)),
#                 (["artist1", "artist2"], 1, (2, 2)),
#                 (["artist1"], 1, (1, 1)),
#             ],
#         )
#         def test_compute_distance_matrix(artists_list, num_chunks, expected_shape):
#             result = compute_distance_matrix(artists_list, num_chunks)

#             assert isinstance(result, csr_matrix), "Result is not a sparse matrix"
#             assert (
#                 result.shape == expected_shape
#             ), "Shape of the matrix is not as expected"
#             assert (
#                 0 <= result.data.nbytes / 1024**2 <= 1
#             ), "Memory used is not within the expected range"

#         @staticmethod
#         @pytest.mark.parametrize(
#             "artists_list, num_chunks",
#             [
#                 ([], 1),
#                 (["artist1", "artist2"], 0),
#             ],
#         )
#         def test_compute_distance_matrix_invalid_input(artists_list, num_chunks):
#             with pytest.raises(ValueError):
#                 compute_distance_matrix(artists_list, num_chunks)
