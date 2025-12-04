import itertools

import torch
from torch import Tensor
from torch.nn import Embedding
from torch.utils.data import DataLoader
from torch_geometric.index import index2ptr
from torch_geometric.typing import EdgeType, NodeType, OptTensor
from torch_geometric.utils import sort_edge_index

EPS = 1e-15


class CustomMetaPath2Vec(torch.nn.Module):
    r"""A custom MetaPath2Vec model that supports multiple metapaths.

    This model samples random walks from multiple metapaths and learns node embeddings
    via negative sampling optimization. This allows learning from different types of
    relationships (e.g., Author-Paper-Author AND Author-Venue-Author) simultaneously.

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

        if num_nodes_dict is None:
            num_nodes_dict = {}
            for keys, edge_index in edge_index_dict.items():
                key = keys[0]
                N = int(edge_index[0].max() + 1)
                num_nodes_dict[key] = max(N, num_nodes_dict.get(key, N))

                key = keys[-1]
                N = int(edge_index[1].max() + 1)
                num_nodes_dict[key] = max(N, num_nodes_dict.get(key, N))

        self.rowptr_dict, self.col_dict, self.rowcount_dict = {}, {}, {}
        for keys, edge_index in edge_index_dict.items():
            sizes = (num_nodes_dict[keys[0]], num_nodes_dict[keys[-1]])
            row, col = sort_edge_index(edge_index, num_nodes=max(sizes)).cpu()
            rowptr = index2ptr(row, size=sizes[0])
            self.rowptr_dict[keys] = rowptr
            self.col_dict[keys] = col
            self.rowcount_dict[keys] = rowptr[1:] - rowptr[:-1]

        # Validate all metapaths
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

        # We don't enforce walk_length vs metapath length as we repeat metapaths
        assert walk_length + 1 >= context_size

        self.embedding_dim = embedding_dim
        self.metapaths = metapaths
        self.walk_length = walk_length
        self.context_size = context_size
        self.walks_per_node = walks_per_node
        self.num_negative_samples = num_negative_samples
        self.num_nodes_dict = num_nodes_dict

        # Collect all types involved
        types = set()
        for metapath in metapaths:
            types.update({x[0] for x in metapath} | {x[-1] for x in metapath})
        types = sorted(types)

        count = 0
        self.start, self.end = {}, {}
        for key in types:
            self.start[key] = count
            count += num_nodes_dict[key]
            self.end[key] = count

        # Calculate offsets for each metapath
        self.offsets = []
        for metapath in metapaths:
            offset = [self.start[metapath[0][0]]]
            offset += [self.start[keys[-1]] for keys in metapath] * int(
                (walk_length / len(metapath)) + 1
            )
            offset = offset[: walk_length + 1]
            assert len(offset) == walk_length + 1
            self.offsets.append(torch.tensor(offset))

        self.embedding = Embedding(count + 1, embedding_dim, sparse=sparse)
        self.dummy_idx = count

        self.reset_parameters()

    def reset_parameters(self):
        r"""Resets all learnable parameters of the module."""
        self.embedding.reset_parameters()

    def forward(self, node_type: str, batch: OptTensor = None) -> Tensor:
        r"""Returns the embeddings for the nodes in :obj:`batch` of type
        :obj:`node_type`.
        """
        emb = self.embedding.weight[self.start[node_type] : self.end[node_type]]
        return emb if batch is None else emb.index_select(0, batch)

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
            # rw[rw > self.dummy_idx] = self.dummy_idx

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

        pos_rw = self._pos_sample(batch)
        neg_rw = self._neg_sample(batch)[: len(pos_rw) * self.num_negative_samples, :]
        return pos_rw, neg_rw

    def loss(self, pos_rw: Tensor, neg_rw: Tensor) -> Tensor:
        r"""Computes the loss given positive and negative random walks."""
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

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"{self.embedding.weight.size(0) - 1}, "
            f"{self.embedding.weight.size(1)})"
        )


def sample(
    rowptr: Tensor,
    col: Tensor,
    rowcount: Tensor,
    subset: Tensor,
    num_neighbors: int,
    dummy_idx: int,
) -> Tensor:
    mask = subset >= dummy_idx
    subset = subset.clamp(min=0, max=rowptr.numel() - 2)
    count = rowcount[subset]

    rand = torch.rand((subset.size(0), num_neighbors), device=subset.device)
    rand *= count.to(rand.dtype).view(-1, 1)
    rand = rand.to(torch.long) + rowptr[subset].view(-1, 1)
    rand = rand.clamp(max=col.numel() - 1)  # If last node is isolated.

    col = col[rand] if col.numel() > 0 else rand
    col[mask | (count == 0)] = dummy_idx
    return col
