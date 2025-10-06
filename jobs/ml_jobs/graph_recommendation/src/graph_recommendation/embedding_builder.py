import sys
import time
from pathlib import Path

import pandas as pd
import torch
from loguru import logger
from torch.optim import Optimizer
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.utils.data import DataLoader
from torch_geometric.nn import Node2Vec

EMBEDDING_DIM = 128
WALK_LENGTH = 20
CONTEXT_SIZE = 10
WALKS_PER_NODE = 10
NUM_NEGATIVE_SAMPLES = 1
P = 1.0
Q = 1.0
NUM_EPOCHS = 15
NUM_WORKERS = 6 if sys.platform == "linux" else 0


def _train(model: Node2Vec, loader: DataLoader, optimizer: Optimizer, device):
    model.train()
    total_loss = 0
    for pos_rw, neg_rw in loader:
        optimizer.zero_grad()
        loss = model.loss(pos_rw.to(device), neg_rw.to(device))
        loss.backward()
        optimizer.step()
        total_loss += loss.item()

    return total_loss / len(loader)


def build_node_embeddings(graph_data_path, num_workers=NUM_WORKERS):
    graph_data = torch.load(graph_data_path, weights_only=False)
    logger.info("Graph info:")
    logger.info(f"  Total nodes: {graph_data.num_nodes}")
    logger.info(f"  Total edges: {graph_data.edge_index.shape[1]}")
    logger.info(f"  Book nodes: {graph_data.book_mask.sum().item()}")
    logger.info(f"  Metadata nodes: {graph_data.metadata_mask.sum().item()}")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info(f"Using device: {device}")
    model = Node2Vec(
        graph_data.edge_index,
        embedding_dim=EMBEDDING_DIM,
        walk_length=WALK_LENGTH,
        context_size=CONTEXT_SIZE,
        walks_per_node=WALKS_PER_NODE,
        num_negative_samples=NUM_NEGATIVE_SAMPLES,
        p=P,
        q=Q,
        sparse=True,
    ).to(device)

    loader = model.loader(batch_size=128, shuffle=True, num_workers=num_workers)
    optimizer = torch.optim.SparseAdam(list(model.parameters()), lr=0.01)

    # Setup callbacks
    scheduler = ReduceLROnPlateau(
        optimizer, mode="min", factor=0.5, patience=3, verbose=True, min_lr=1e-6
    )

    # Model checkpoint setup
    checkpoint_dir = Path("checkpoints")
    checkpoint_dir.mkdir(exist_ok=True)
    checkpoint_path = checkpoint_dir / "best_node2vec_model.pt"
    best_loss = float("inf")

    # Train Node2Vec (unsupervised - no labels needed)
    for epoch in range(1, NUM_EPOCHS + 1):
        t0 = time.time()
        loss = _train(model, loader, optimizer, device)
        logger.info(
            f"Epoch: {epoch:03d}, Loss: {loss:.4f}, "
            f"LR: {optimizer.param_groups[0]['lr']:.6f}, Time: {time.time() - t0:.2f}s"
        )

        # ReduceLROnPlateau step
        scheduler.step(loss)

        # Model checkpoint: save best model
        if loss < best_loss:
            best_loss = loss
            torch.save(
                {
                    "epoch": epoch,
                    "model_state_dict": model.state_dict(),
                    "optimizer_state_dict": optimizer.state_dict(),
                    "loss": loss,
                },
                checkpoint_path,
            )
            logger.info(f"Saved best model with loss: {loss:.4f}")

    # Load best model weights
    logger.info(f"Loading best model from {checkpoint_path}")
    checkpoint = torch.load(checkpoint_path, weights_only=False)
    model.load_state_dict(checkpoint["model_state_dict"])
    logger.info(
        f"Best model from epoch {checkpoint['epoch']} "
        f"with loss {checkpoint['loss']:.4f}"
    )

    # Get embeddings for all nodes
    model.eval()
    with torch.no_grad():
        embeddings = model()

    # Format resuts in a DataFrame
    return (
        pd.DataFrame(
            {
                "node_ids": graph_data.node_ids,
                "embeddings": list(embeddings.cpu().detach().numpy()),
                "node_type_id": graph_data.node_type.numpy().tolist(),
            }
        )
        .assign(
            node_type=lambda df: df["node_type_id"].map(
                {value: key for key, value in graph_data.metadata_type_to_id.items()}
            )
        )
        .astype(
            {
                "node_ids": str,
            }
        )
    )
