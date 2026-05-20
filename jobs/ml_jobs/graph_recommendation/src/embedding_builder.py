import time
from datetime import timedelta
from pathlib import Path

import mlflow
import pandas as pd
import torch
from loguru import logger
from torch.optim import Optimizer
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.utils.data import DataLoader
from torch_geometric.data import HeteroData

from src.config import TrainingConfig
from src.constants import EMBEDDING_COLUMN
from src.custom_metapath2vec import CustomMetaPath2Vec
from src.utils.mlflow import (
    conditional_mlflow,
    log_model_parameters,
)


@conditional_mlflow()
def _train(
    model: torch.nn.Module,
    loader: DataLoader,
    optimizer: Optimizer,
    device,
    epoch: int,
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
    for batch_idx, (pos_rw, neg_rw) in enumerate(loader):
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

        batch_loss = loss.item()
        total_loss += batch_loss

        # Log batch loss every 100 batches to MLflow
        if batch_idx % 100 == 0:
            global_step = (epoch - 1) * len(loader) + batch_idx
            mlflow.log_metric("batch_loss", batch_loss, step=global_step)

    if profile:
        total_time = sum(timings.values())
        logger.info("Profiling breakdown:")
        for key, val in timings.items():
            logger.info(f"  {key}: {val:.2f}s ({val / total_time * 100:.1f}%)")

    return total_loss / len(loader)


@conditional_mlflow()
def train_metapath2vec(
    graph_data: HeteroData,
    training_config: TrainingConfig,
    checkpoint_path: Path = Path("checkpoints/best_metapath2vec_model.pt"),
    *,
    profile: bool = False,
) -> pd.DataFrame:
    """
    Train MetaPath2Vec and return embeddings with gtl_id.

    Returns:
        DataFrame with ['item_id', 'gtl_id', EMBEDDING_COLUMN_NAME] columns
    """

    logger.info("Training configuration:")
    logger.info(training_config.to_dict())

    logger.info("Graph info:")
    logger.info(f"  Node types: {graph_data.node_types}")
    logger.info(f"  Edge types: {graph_data.edge_types}")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info(f"Using device: {device}")

    # Restrict metapaths to those that exist in the graph.
    # A metapath is valid when ALL node types it references are present in the
    # graph.  We collect every unique node type across all edges of the metapath
    # and check that each one exists as a node type in graph_data.
    all_node_types = set(graph_data.node_types)
    valid_metapaths = [
        metapath
        for metapath in training_config.metapaths
        if all(
            node_type in all_node_types
            for edge in metapath
            for node_type in (edge[0], edge[2])
        )
    ]
    logger.info(f"Using these valid metapaths for training: {valid_metapaths}")

    model = CustomMetaPath2Vec(
        graph_data.edge_index_dict,
        embedding_dim=training_config.embedding_dim,
        metapaths=valid_metapaths,
        walk_length=training_config.walk_length,
        context_size=training_config.context_size,
        walks_per_node=training_config.walks_per_node,
        num_negative_samples=training_config.num_negative_samples,
        sparse=True,
    ).to(device)

    loader = model.loader(
        batch_size=training_config.batch_size,
        shuffle=True,
        num_workers=training_config.num_workers,
        pin_memory=True,
        persistent_workers=training_config.num_workers > 0,
    )
    optimizer = torch.optim.SparseAdam(
        list(model.parameters()),
        lr=training_config.learning_rate,
    )
    scheduler = ReduceLROnPlateau(
        optimizer, mode="min", factor=0.5, patience=1, min_lr=1e-6, threshold=0.01
    )

    # Log model parameters in mlflow
    log_model_parameters(
        training_config.to_dict() | {"walk_length": training_config.walk_length}
    )

    # Start training
    logger.info("Starting training...")
    checkpoint_dir = checkpoint_path.parent
    checkpoint_dir.mkdir(exist_ok=True)
    best_loss = float("inf")
    training_start = time.time()
    best_loss_epoch = 0
    for epoch in range(1, training_config.num_epochs + 1):
        t0 = time.time()
        loss = _train(model, loader, optimizer, device, epoch, profile=profile)
        epoch_time = time.time() - t0

        logger.info(
            f"Epoch: {epoch:03d}, Loss: {loss:.4f}, "
            f"LR: {optimizer.param_groups[0]['lr']:.6f}, Time: {epoch_time:.2f}s"
        )

        # Log epoch metrics to MLflow
        mlflow.log_metrics(
            {
                "epoch_loss": loss,
                "learning_rate": optimizer.param_groups[0]["lr"],
                "epoch_time": epoch_time,
            },
            step=epoch,
        )

        scheduler.step(loss)

        prev_best_loss = best_loss
        if loss < best_loss:
            best_loss = loss
            torch.save(model.state_dict(), checkpoint_path)
            logger.info(f"Saved best model with loss: {loss:.4f}")
            mlflow.log_metric("best_loss", best_loss, step=epoch)
            best_loss_epoch += 1

        # Early stopping check
        if (
            abs(prev_best_loss - best_loss) < training_config.early_stopping_delta
            and training_config.early_stop
        ):
            break

    # Log total training time and final best loss
    total_training_time = time.time() - training_start
    mlflow.log_metric("total_training_time", total_training_time)
    mlflow.log_metric("final_best_loss", best_loss)
    if training_config.early_stop:
        mlflow.log_metric("stop_epoch", best_loss_epoch)
    else:
        mlflow.log_metric("stop_epoch", epoch)

    time_formatted = str(timedelta(seconds=int(total_training_time)))
    logger.info(f"Training completed in {time_formatted}")

    # Extract embeddings for all items from the single "item" node type.
    # item_ids_by_type maps each item_type to the ordered list of its item_ids,
    # which correspond to contiguous slices within the global "item" node range.
    logger.info("Extracting item embeddings...")
    checkpoint = torch.load(checkpoint_path, weights_only=True)
    embedding = checkpoint["embedding.weight"].detach().cpu().numpy()

    # The node type in the model is always "item" in the multi-type graph.
    # Fall back to "book" for legacy single-type graphs.
    item_node_type = "item" if "item" in model.start else "book"

    item_ids_by_type: dict[str, list] = getattr(
        graph_data,
        "item_ids_by_type",
        {item_node_type: graph_data.book_ids},
    )
    gtl_ids_by_type: dict[str, list] = getattr(
        graph_data,
        "gtl_ids_by_type",
        {item_node_type: graph_data.gtl_ids},
    )

    if item_node_type not in model.start:
        raise RuntimeError(
            f"Node type '{item_node_type}' not found in model.start: {list(model.start.keys())}"
        )

    start_idx = model.start[item_node_type]
    all_item_embeddings = embedding[
        start_idx : start_idx + graph_data[item_node_type].num_nodes, :
    ]

    # item_ids_by_type contains per-type slices in the same order as the
    # global item node list built by the heterograph builder (sorted by
    # item_type then item_id), so we can slice the embedding matrix directly.
    all_rows: list[dict] = []
    offset = 0
    for item_type in sorted(item_ids_by_type.keys()):
        item_ids = item_ids_by_type[item_type]
        gtl_ids = gtl_ids_by_type.get(item_type, [None] * len(item_ids))
        type_embeddings = all_item_embeddings[offset : offset + len(item_ids), :]
        offset += len(item_ids)
        for node_id, gtl_id, emb in zip(
            item_ids, gtl_ids, type_embeddings, strict=False
        ):
            all_rows.append(
                {
                    "node_ids": node_id,
                    "gtl_id": gtl_id,
                    "item_type": item_type,
                    EMBEDDING_COLUMN: emb,
                }
            )

    embeddings_df = pd.DataFrame(all_rows)
    logger.info(
        f"Embeddings extracted: {len(embeddings_df)} items "
        f"({', '.join(f'{t}: {(embeddings_df.item_type == t).sum()}' for t in item_ids_by_type)})"
    )
    return embeddings_df
