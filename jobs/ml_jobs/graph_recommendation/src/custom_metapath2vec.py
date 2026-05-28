import itertools

import torch
from torch import Tensor
from torch.nn import Embedding
from torch.utils.data import DataLoader
from torch_geometric.index import index2ptr
from torch_geometric.nn.models.metapath2vec import sample
from torch_geometric.typing import EdgeType, NodeType
from torch_geometric.utils import sort_edge_index

EPS = 1e-15


class CustomMetaPath2Vec(torch.nn.Module):
    r"""A custom MetaPath2Vec model that supports multiple metapaths.

    Largely inspired from torch_geometric.nn.models.metapath2vec.MetaPath2Vec.

    This model samples random walks from multiple metapaths and learns node embeddings
    via negative sampling optimization. This allows learning from different types of
    relationships (e.g., Author-Paper-Author AND Author-Venue-Author) simultaneously.

    If a metapath is stopped because it reached a dummy node, the corresponding walk is
    discarded.

    Args:
        edge_index_dict (Dict[Tuple[str, str, str], torch.Tensor]): Dictionary
            holding edge indices for each edge type.
        embedding_dim (int): The size of each embedding vector.
        metapaths (List[List[Tuple[str, str, str]]]): A list of metapaths, where
            each metapath is a list of (src, rel, dst) tuples.
        walk_length (int): The walk length.
        context_size (int): The actual context size which is considered for
            positive samples.
        walks_per_node (int, optional): The number of walks to sample for each
            node. (default: :obj:`1`)
        num_negative_samples (int, optional): The number of negative samples to
            use for each positive sample. (default: :obj:`1`)
        num_nodes_dict (Dict[str, int], optional): Dictionary holding the
            number of nodes for each node type. (default: :obj:`None`)
        sparse (bool, optional): If set to :obj:`True`, gradients w.r.t. to the
            weight matrix will be sparse. (default: :obj:`False`)
    """

    def __init__(
        self,
        edge_index_dict: dict[EdgeType, Tensor],
        embedding_dim: int,
        metapaths: list[list[EdgeType]],
        walk_length: int,
        context_size: int,
        walks_per_node: int = 1,
        num_negative_samples: int = 1,
        num_nodes_dict: dict[NodeType, int] | None = None,
        *,
        sparse: bool = False,
    ):
        super().__init__()

        # Validate metapaths and walk lengths
        self._validate_metapaths(metapaths)
        assert walk_length + 1 >= context_size

        self.embedding_dim = embedding_dim
        self.metapaths = metapaths
        self.walk_length = walk_length
        self.context_size = context_size
        self.walks_per_node = walks_per_node
        self.num_negative_samples = num_negative_samples
        self.num_nodes_dict = self._get_num_nodes_dict(num_nodes_dict, edge_index_dict)
        self.rowptr_dict, self.col_dict, self.rowcount_dict = self._get_sparse_matrix(
            edge_index_dict, self.num_nodes_dict
        )
        self.start, self.end, count = self._get_node_type_ranges(
            self.metapaths, self.num_nodes_dict
        )
        self.offsets = self._compute_offsets(
            self.metapaths, self.start, self.walk_length
        )
        self.embedding = Embedding(count + 1, embedding_dim, sparse=sparse)
        self.dummy_idx = count

        self.reset_parameters()

    def loader(self, **kwargs):
        r"""Returns the data loader that creates both positive and negative
        random walks on the heterogeneous graph.
        """
        # Use the start node type of the first metapath (all are same)
        return DataLoader(
            range(self.num_nodes_dict[self.metapaths[0][0][0]]),
            collate_fn=self._sample,
            **kwargs,
        )

    def _pos_sample(self, batch: Tensor) -> Tensor:
        batch = batch.repeat(self.walks_per_node)
        all_walks = []

        for i, metapath in enumerate(self.metapaths):
            # Important: Use a copy/fresh batch for each metapath iteration
            # because sample() modifies the batch (returns next nodes)
            curr_batch = batch
            rws = [curr_batch]

            for k in range(self.walk_length):
                edge_type = metapath[k % len(metapath)]
                curr_batch = sample(
                    self.rowptr_dict[edge_type],
                    self.col_dict[edge_type],
                    self.rowcount_dict[edge_type],
                    curr_batch,
                    num_neighbors=1,
                    dummy_idx=self.dummy_idx,
                ).view(-1)
                rws.append(curr_batch)

            rw = torch.stack(rws, dim=-1)

            # Apply offset for this specific metapath
            # Ensure offset is on the same device as rw
            offset = self.offsets[i].to(rw.device)
            rw.add_(offset.view(1, -1))

            # Remove walks with dummy nodes
            rw = rw[(rw < self.dummy_idx).all(axis=1), :]

            walks = []
            num_walks_per_rw = 1 + self.walk_length + 1 - self.context_size
            for j in range(num_walks_per_rw):
                walks.append(rw[:, j : j + self.context_size])

            all_walks.append(torch.cat(walks, dim=0))
        return torch.cat(all_walks, dim=0)

    def _neg_sample(self, batch: Tensor) -> Tensor:
        batch = batch.repeat(self.walks_per_node * self.num_negative_samples)

        all_walks = []

        for i, metapath in enumerate(self.metapaths):
            curr_batch = batch
            rws = [curr_batch]

            for k in range(self.walk_length):
                keys = metapath[k % len(metapath)]
                curr_batch = torch.randint(
                    0,
                    self.num_nodes_dict[keys[-1]],
                    (curr_batch.size(0),),
                    dtype=torch.long,
                    device=curr_batch.device,
                )
                rws.append(curr_batch)

            rw = torch.stack(rws, dim=-1)

            # Apply offset for this specific metapath
            offset = self.offsets[i].to(rw.device)
            rw.add_(offset.view(1, -1))

            walks = []
            num_walks_per_rw = 1 + self.walk_length + 1 - self.context_size
            for j in range(num_walks_per_rw):
                walks.append(rw[:, j : j + self.context_size])

            all_walks.append(torch.cat(walks, dim=0))

        return torch.cat(all_walks, dim=0)

    def _sample(self, batch: list[int]) -> tuple[Tensor, Tensor]:
        if not isinstance(batch, Tensor):
            batch = torch.tensor(batch, dtype=torch.long)

        batch_pos = batch.repeat(self.walks_per_node)
        all_pos_walks = []
        all_neg_walks = []

        num_walks_per_rw = 1 + self.walk_length + 1 - self.context_size

        for i, metapath in enumerate(self.metapaths):
            # 1. Generate Positive Walk
            curr_batch_pos = batch_pos
            rws_pos = [curr_batch_pos]

            for k in range(self.walk_length):
                edge_type = metapath[k % len(metapath)]
                curr_batch_pos = sample(
                    self.rowptr_dict[edge_type],
                    self.col_dict[edge_type],
                    self.rowcount_dict[edge_type],
                    curr_batch_pos,
                    num_neighbors=1,
                    dummy_idx=self.dummy_idx,
                ).view(-1)
                rws_pos.append(curr_batch_pos)

            rw_pos = torch.stack(rws_pos, dim=-1)

            # Check which walks survived (no dummy nodes)
            keep_mask = (rw_pos < self.dummy_idx).all(dim=1)
            rw_pos_survived = rw_pos[keep_mask]

            num_survived = rw_pos_survived.size(0)
            if num_survived == 0:
                continue

            # 2. Generate Negative walks ONLY for survived positive walks
            # The start node is the first node of each survived walk
            start_nodes = rw_pos_survived[:, 0]
            curr_batch_neg = start_nodes.repeat_interleave(self.num_negative_samples)
            rws_neg = [curr_batch_neg]

            for k in range(self.walk_length):
                keys = metapath[k % len(metapath)]
                curr_batch_neg = torch.randint(
                    0,
                    self.num_nodes_dict[keys[-1]],
                    (curr_batch_neg.size(0),),
                    dtype=torch.long,
                    device=curr_batch_neg.device,
                )
                rws_neg.append(curr_batch_neg)

            rw_neg = torch.stack(rws_neg, dim=-1)

            # Apply offset to both positive and negative walks
            offset = self.offsets[i].to(rw_pos.device)
            rw_pos_survived_offset = rw_pos_survived + offset.view(1, -1)
            rw_neg_offset = rw_neg + offset.view(1, -1)

            # Slice into windows of context_size
            walks_pos = []
            for j in range(num_walks_per_rw):
                walks_pos.append(rw_pos_survived_offset[:, j : j + self.context_size])
            all_pos_walks.append(torch.cat(walks_pos, dim=0))

            walks_neg = []
            for j in range(num_walks_per_rw):
                walks_neg.append(rw_neg_offset[:, j : j + self.context_size])
            all_neg_walks.append(torch.cat(walks_neg, dim=0))

        if not all_pos_walks:
            empty_tensor = torch.empty(
                (0, self.context_size), dtype=torch.long, device=batch.device
            )
            return empty_tensor, empty_tensor

        pos_rw = torch.cat(all_pos_walks, dim=0)
        neg_rw = torch.cat(all_neg_walks, dim=0)
        return pos_rw, neg_rw

    @staticmethod
    def _validate_metapaths(metapaths: list[list[EdgeType]]) -> None:
        if not metapaths:
            raise ValueError("At least one metapath must be provided.")

        start_node_type = metapaths[0][0][0]

        for metapath in metapaths:
            if metapath[0][0] != start_node_type:
                raise ValueError(
                    f"All metapaths must start with the same "
                    f"node type '{start_node_type}'. "
                    f"Found start type '{metapath[0][0]}' in one of the metapaths."
                )

            for edge_type1, edge_type2 in itertools.pairwise(metapath):
                if edge_type1[-1] != edge_type2[0]:
                    raise ValueError(
                        "Found invalid metapath. Ensure that the destination node "
                        "type matches with the source node type across all "
                        "consecutive edge types."
                    )

    @staticmethod
    def _get_num_nodes_dict(num_nodes_dict, edge_index_dict) -> dict[NodeType, int]:
        if num_nodes_dict is None:
            num_nodes_dict = {}
            for keys, edge_index in edge_index_dict.items():
                key = keys[0]
                N = int(edge_index[0].max() + 1)
                num_nodes_dict[key] = max(N, num_nodes_dict.get(key, N))

                key = keys[-1]
                N = int(edge_index[1].max() + 1)
                num_nodes_dict[key] = max(N, num_nodes_dict.get(key, N))

            return num_nodes_dict
        return num_nodes_dict

    @staticmethod
    def _get_sparse_matrix(
        edge_index_dict: dict[EdgeType, Tensor],
        num_nodes_dict: dict[NodeType, int],
    ) -> tuple[dict[EdgeType, Tensor], dict[EdgeType, Tensor], dict[EdgeType, Tensor]]:
        rowptr_dict, col_dict, rowcount_dict = {}, {}, {}
        for keys, edge_index in edge_index_dict.items():
            sizes = (num_nodes_dict[keys[0]], num_nodes_dict[keys[-1]])
            row, col = sort_edge_index(edge_index, num_nodes=max(sizes)).cpu()
            rowptr = index2ptr(row, size=sizes[0])
            rowptr_dict[keys] = rowptr
            col_dict[keys] = col
            rowcount_dict[keys] = rowptr[1:] - rowptr[:-1]
        return rowptr_dict, col_dict, rowcount_dict

    @staticmethod
    def _get_node_type_ranges(
        metapaths: list[list[EdgeType]], num_nodes_dict: dict[NodeType, int]
    ) -> tuple[dict[NodeType, int], dict[NodeType, int], int]:
        # Collect all types involved
        types = set()
        for metapath in metapaths:
            types.update({x[0] for x in metapath} | {x[-1] for x in metapath})
        types = sorted(types)

        count = 0
        start, end = {}, {}
        for key in types:
            start[key] = count
            count += num_nodes_dict[key]
            end[key] = count

        return start, end, count

    @staticmethod
    def _compute_offsets(
        metapaths: list[list[EdgeType]],
        start: dict[NodeType, int],
        walk_length: int,
    ) -> list[Tensor]:
        # Calculate offsets for each metapath
        offsets = []
        for metapath in metapaths:
            offset = [start[metapath[0][0]]]
            offset += [start[keys[-1]] for keys in metapath] * int(
                (walk_length / len(metapath)) + 1
            )
            offset = offset[: walk_length + 1]
            assert len(offset) == walk_length + 1
            offsets.append(torch.tensor(offset))

        return offsets

    ###############################################################################
    ##########       Below methods are unchanged from MetaPath2Vec       ##########
    ###############################################################################

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"{self.embedding.weight.size(0) - 1}, "
            f"{self.embedding.weight.size(1)})"
        )

    def reset_parameters(self):
        self.embedding.reset_parameters()

    def forward(self, node_type: str, batch: Tensor) -> Tensor:
        r"""Returns the embeddings for the given node type and batch."""
        return self.embedding(batch + self.start[node_type])

    def loss(self, pos_rw: Tensor, neg_rw: Tensor) -> Tensor:
        r"""Computes the loss given positive and negative random walks."""

        if pos_rw.numel() == 0:
            return torch.tensor(0.0, device=pos_rw.device, requires_grad=True)

        # Positive loss.
        start, rest = pos_rw[:, 0], pos_rw[:, 1:].contiguous()

        h_start = self.embedding(start).view(pos_rw.size(0), 1, self.embedding_dim)
        h_rest = self.embedding(rest.view(-1)).view(
            pos_rw.size(0), -1, self.embedding_dim
        )

        out = (h_start * h_rest).sum(dim=-1).view(-1)
        pos_loss = -torch.log(torch.sigmoid(out) + EPS).mean()

        # Negative loss.
        start, rest = neg_rw[:, 0], neg_rw[:, 1:].contiguous()

        h_start = self.embedding(start).view(neg_rw.size(0), 1, self.embedding_dim)
        h_rest = self.embedding(rest.view(-1)).view(
            neg_rw.size(0), -1, self.embedding_dim
        )

        out = (h_start * h_rest).sum(dim=-1).view(-1)
        neg_loss = -torch.log(1 - torch.sigmoid(out) + EPS).mean()

        return pos_loss + neg_loss
