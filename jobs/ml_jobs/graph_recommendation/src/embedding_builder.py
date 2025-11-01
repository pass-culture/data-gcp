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
from torch_geometric.nn import MetaPath2Vec

from src.constants import EMBEDDING_COLUMN, DefaultTrainingConfig, InvalidConfigError
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
    checkpoint_path: Path = Path("checkpoints/best_metapath2vec_model.pt"),
    train_params: DefaultTrainingConfig | dict | None = None,
    *,
    profile: bool = False,
) -> pd.DataFrame:
    """
    Train MetaPath2Vec and return embeddings with gtl_id.

    Returns:
        DataFrame with ['item_id', 'gtl_id', EMBEDDING_COLUMN_NAME] columns
    """

    # Merge config with defaults
    if train_params is None:
        params = DefaultTrainingConfig()
    elif isinstance(train_params, DefaultTrainingConfig):
        params = train_params
    elif isinstance(train_params, dict):
        params = DefaultTrainingConfig()
        params.update_from_dict(train_params)
    else:
        raise InvalidConfigError(
            f"train_params must be DefaultTrainingConfig, dict, or None, "
            f"got {type(train_params).__name__}"
        )
    logger.info("Training configuration:")
    logger.info(params.to_dict())

    logger.info("Graph info:")
    logger.info(f"  Node types: {graph_data.node_types}")
    logger.info(f"  Edge types: {graph_data.edge_types}")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info(f"Using device: {device}")

    # Retrieve parameters from params dict
    embedding_dim = params.embedding_dim
    metapath = params.metapath
    context_size = params.context_size
    walk_length = int(len(params.metapath) * params.metapath_repetitions)
    walks_per_node = params.walks_per_node
    num_negative_samples = params.num_negative_samples
    batch_size = params.batch_size
    learning_rate = params.learning_rate
    num_epochs = params.num_epochs
    early_stop = params.early_stop
    num_workers = params.num_workers

    model = MetaPath2Vec(
        graph_data.edge_index_dict,
        embedding_dim=embedding_dim,
        metapath=metapath,
        walk_length=walk_length,
        context_size=context_size,
        walks_per_node=walks_per_node,
        num_negative_samples=num_negative_samples,
        sparse=True,
    ).to(device)

    loader = model.loader(
        batch_size=batch_size,
        shuffle=True,
        num_workers=num_workers,
        pin_memory=True,
        persistent_workers=num_workers > 0,
    )
    optimizer = torch.optim.SparseAdam(
        list(model.parameters()),
        lr=learning_rate,
    )
    scheduler = ReduceLROnPlateau(
        optimizer, mode="min", factor=0.5, patience=3, min_lr=1e-6
    )

    # Log model parameters in mlflowww
    log_model_parameters(params.to_dict() | {"walk_length": walk_length})

    # Start training
    logger.info("Starting training...")
    checkpoint_dir = checkpoint_path.parent
    checkpoint_dir.mkdir(exist_ok=True)
    best_loss = float("inf")
    training_start = time.time()
    best_loss_epoch = 0
    for epoch in range(1, num_epochs + 1):
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

        if loss < best_loss:
            best_loss = loss
            torch.save(model.state_dict(), checkpoint_path)
            logger.info(f"Saved best model with loss: {loss:.4f}")
            mlflow.log_metric("best_loss", best_loss, step=epoch)
            best_loss_epoch += 1
        elif early_stop:
            break

    # Log total training time and final best loss
    total_training_time = time.time() - training_start
    mlflow.log_metric("total_training_time", total_training_time)
    mlflow.log_metric("final_best_loss", best_loss)
    if early_stop:
        mlflow.log_metric("stop_epoc", best_loss_epoch)
    else:
        mlflow.log_metric("stop_epoc", epoch)

    time_formatted = str(timedelta(seconds=int(total_training_time)))
    logger.info(f"Training completed in {time_formatted}")

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
            EMBEDDING_COLUMN: list(book_embeddings),
        }
    )

    logger.info(f"Book embeddings extracted: {len(embeddings_df)} items with gtl_id")
    return embeddings_df
