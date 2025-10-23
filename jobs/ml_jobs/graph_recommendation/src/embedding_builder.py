import sys
import time
from pathlib import Path

import pandas as pd
import torch
from loguru import logger
from torch.optim import Optimizer
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.utils.data import DataLoader
from torch_geometric.data import HeteroData
from torch_geometric.nn import MetaPath2Vec

EMBEDDING_DIM: int = 32  # DEBUG: should try 128 for better results once metrics are on
WALK_LENGTH = 14 * 2  # DEBUG: should try 14*4 for better results once metrics are on
CONTEXT_SIZE = 10
WALKS_PER_NODE = 5
NUM_NEGATIVE_SAMPLES = 5
NUM_EPOCHS = 5  # DEBUG: should try 15 for better results once metrics are on
NUM_WORKERS = 12 if sys.platform == "linux" else 0
BATCH_SIZE = 256
LEARNING_RATE = 0.01
METAPATH = (
    4
    * [
        ("book", "is_artist_id", "artist_id"),
        ("artist_id", "artist_id_of", "book"),
    ]
    + 4
    * [
        ("book", "is_gtl_label_level_4", "gtl_label_level_4"),
        ("gtl_label_level_4", "gtl_label_level_4_of", "book"),
    ]
    + 3
    * [
        ("book", "is_gtl_label_level_3", "gtl_label_level_3"),
        ("gtl_label_level_3", "gtl_label_level_3_of", "book"),
    ]
    + 2
    * [
        ("book", "is_gtl_label_level_2", "gtl_label_level_2"),
        ("gtl_label_level_2", "gtl_label_level_2_of", "book"),
    ]
    + [
        ("book", "is_gtl_label_level_1", "gtl_label_level_1"),
        ("gtl_label_level_1", "gtl_label_level_1_of", "book"),
    ]
)


def _train(
    model: torch.nn.Module,
    loader: DataLoader,
    optimizer: Optimizer,
    device,
    *,
    profile: bool,
):
    model.train()
    total_loss = 0
    timings = {
        "data_load": 0,
        "to_device": 0,
        "forward": 0,
        "backward": 0,
        "optimizer": 0,
    }

    batch_start = time.time()
    for pos_rw, neg_rw in loader:
        if profile:
            data_time = time.time() - batch_start
            timings["data_load"] += data_time

            t0 = time.time()

        optimizer.zero_grad()
        pos_rw_device = pos_rw.to(device, non_blocking=True)
        neg_rw_device = neg_rw.to(device, non_blocking=True)

        if profile:
            timings["to_device"] += time.time() - t0
            t0 = time.time()

        loss = model.loss(pos_rw_device, neg_rw_device)

        if profile:
            timings["forward"] += time.time() - t0
            t0 = time.time()

        loss.backward()

        if profile:
            timings["backward"] += time.time() - t0
            t0 = time.time()

        optimizer.step()

        if profile:
            timings["optimizer"] += time.time() - t0
            batch_start = time.time()

        total_loss += loss.item()

    if profile:
        total_time = sum(timings.values())
        logger.info("Profiling breakdown:")
        for key, val in timings.items():
            logger.info(f"  {key}: {val:.2f}s ({val / total_time * 100:.1f}%)")

    return total_loss / len(loader)


def train_metapath2vec(
    graph_data: HeteroData,
    checkpoint_path: Path = Path("checkpoints/best_metapath2vec_model.pt"),
    num_workers=NUM_WORKERS,
    profile=False,
) -> pd.DataFrame:
    """
    Train MetaPath2Vec and return embeddings with gtl_id.

    Returns:
        DataFrame with ['item_id', 'gtl_id', 'embeddings']
    """
    logger.info("Graph info:")
    logger.info(f"  Node types: {graph_data.node_types}")
    logger.info(f"  Edge types: {graph_data.edge_types}")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info(f"Using device: {device}")

    model = MetaPath2Vec(
        graph_data.edge_index_dict,
        embedding_dim=EMBEDDING_DIM,
        metapath=METAPATH,
        walk_length=WALK_LENGTH,
        context_size=CONTEXT_SIZE,
        walks_per_node=WALKS_PER_NODE,
        num_negative_samples=NUM_NEGATIVE_SAMPLES,
        sparse=True,
    ).to(device)

    loader = model.loader(
        batch_size=BATCH_SIZE,
        shuffle=True,
        num_workers=num_workers,
        pin_memory=True,
        persistent_workers=num_workers > 0,
    )
    optimizer = torch.optim.SparseAdam(
        list(model.parameters()),
        lr=LEARNING_RATE,
    )
    scheduler = ReduceLROnPlateau(
        optimizer, mode="min", factor=0.5, patience=3, min_lr=1e-6
    )

    # Start training
    logger.info("Starting training...")
    checkpoint_dir = checkpoint_path.parent
    checkpoint_dir.mkdir(exist_ok=True)
    best_loss = float("inf")
    for epoch in range(1, NUM_EPOCHS + 1):
        t0 = time.time()
        loss = _train(model, loader, optimizer, device, profile=profile)
        logger.info(
            f"Epoch: {epoch:03d}, Loss: {loss:.4f}, "
            f"LR: {optimizer.param_groups[0]['lr']:.6f}, Time: {time.time() - t0:.2f}s"
        )
        scheduler.step(loss)
        if loss < best_loss:
            best_loss = loss
            torch.save(model.state_dict(), checkpoint_path)
            logger.info(f"Saved best model with loss: {loss:.4f}")
    logger.info("Training completed.")

    # Extract and save embeddings for book nodes
    logger.info("Extracting book embeddings...")
    checkpoint = torch.load(checkpoint_path, weights_only=True)
    embedding = checkpoint["embedding.weight"].detach().cpu().numpy()
    book_embeddings = embedding[
        model.start["book"] : model.start["book"] + graph_data["book"].num_nodes, :
    ]

    embeddings_df = pd.DataFrame(
        {
            "node_ids": graph_data.book_ids,
            "gtl_id": graph_data.gtl_ids,
            "embeddings": list(book_embeddings),
        }
    )

    logger.info(f"Book embeddings extracted: {len(embeddings_df)} items with gtl_id")
    return embeddings_df
