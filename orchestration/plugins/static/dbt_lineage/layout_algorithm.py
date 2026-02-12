"""
Force-directed layout algorithm for dbt lineage visualization.

This module implements a numpy-based force simulation that computes stable
node positions. It's designed to work both server-side (Python) and
client-side (Pyodide in browser).

The algorithm applies multiple forces:
1. Repulsion: All nodes push each other apart (Coulomb's law)
2. Attraction: Connected nodes pull together (Hooke's law / spring)
3. Tier constraint: Lock nodes to horizontal tier columns
4. Hierarchy: Group related folders together
5. Center gravity: Prevent drift from canvas center
"""

import json
import os
from pathlib import Path
from typing import Any

import numpy as np


def get_airflow_home() -> str:
    """
    Get the Airflow home directory based on environment.

    Returns:
        Path to Airflow home directory
    """
    if os.environ.get("LOCAL_ENV", None) == "1":
        return "/opt/airflow/"
    if os.environ.get("DAG_FOLDER", None) == "/opt/airflow/dags":
        return "/opt/airflow/"
    if os.environ.get("DAG_FOLDER", None) == "/home/airflow/gcs/dags":
        return "/home/airflow/gcs"

    # Import here to avoid Pyodide errors (only used server-side)
    try:
        from airflow.exceptions import AirflowConfigException

        raise AirflowConfigException(
            "Airflow home not found, failed to determine environment"
        )
    except ImportError:
        # Running in Pyodide/browser - return default (won't be used)
        return "/opt/airflow/"


def load_config_from_yaml() -> dict:
    """
    Load configuration from config.yaml (single source of truth).

    Returns:
        Dictionary with all configuration values loaded from YAML.

    Raises:
        FileNotFoundError: If config.yaml is not found
    """
    import yaml  # Import here to avoid Pyodide errors (only used server-side)

    airflow_home = get_airflow_home()
    config_path = Path(airflow_home) / "plugins/static/dbt_lineage/config.yaml"

    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        yaml_config = yaml.safe_load(f)

    # Extract dimensions
    dimensions = yaml_config.get("project", {}).get("dimensions", {})
    config = {
        "width": dimensions.get("width", 1920),
        "height": dimensions.get("height", 1080),
    }

    # Extract force parameters from forces section
    forces = yaml_config.get("forces", {})
    for key, params in forces.items():
        if isinstance(params, dict) and "val" in params:
            config[key] = params["val"]

    # Extract default per-force node exemptions
    config["disabled_forces"] = {
        force: list(node_ids)
        for force, node_ids in yaml_config.get("disabled_forces", {}).items()
    }

    return config


# Load config from YAML (single source of truth).
# Fallback to minimal defaults only for Pyodide (browser JS passes config anyway).
try:
    DEFAULT_CONFIG = load_config_from_yaml()
except Exception:
    # Minimal fallback for Pyodide - not used in practice (JS passes config)
    DEFAULT_CONFIG = {}


def compute_layout(
    nodes: list[dict],
    links: list[dict],
    struct_links: list[dict],
    tiers: list[dict],
    config: dict | None = None,
    disabled_forces: dict[str, list[str]] | None = None,
) -> list[list[float]]:
    """
    Compute force-directed layout positions for nodes.

    Args:
        nodes: List of node dicts with 'id', 'tier', 'count'
        links: List of edge dicts with 'source', 'target', 'value'
        struct_links: List of structural links with 'source', 'target', 'depth'
        tiers: List of tier configs with 'x_percent'
        config: Force parameters (uses DEFAULT_CONFIG if not provided)
        disabled_forces: Optional dict mapping force names to lists of node IDs
            for which that force should be skipped. Supported force names:
            'repulsion', 'attraction', 'hierarchy', 'collision', 'tier', 'gravity'.
            Example: {"tier": ["src.raw", "snapshot"], "gravity": ["export"]}
            This allows pinning specific nodes or freeing them from constraints.

    Returns:
        List of [x, y] positions for each node
    """
    if config is None:
        config = DEFAULT_CONFIG.copy()
    else:
        # Merge with defaults so callers only need to pass overrides
        merged = DEFAULT_CONFIG.copy()
        merged.update(config)
        config = merged

    n = len(nodes)
    if n == 0:
        return []

    disabled_forces = disabled_forces or {}

    # Pre-compute per-force boolean masks (True = force is active for that node index)
    # This avoids repeated set lookups inside the hot simulation loop
    node_ids = [node["id"] for node in nodes]
    id_to_idx = {node_id: idx for idx, node_id in enumerate(node_ids)}

    def _active_mask(force_name: str) -> np.ndarray:
        """Return a boolean array: True where the force is active for each node."""
        excluded_ids = set(disabled_forces.get(force_name, []))
        if not excluded_ids:
            return np.ones(n, dtype=bool)
        return np.array([node["id"] not in excluded_ids for node in nodes])

    mask_repulsion = _active_mask("repulsion")
    mask_attraction = _active_mask("attraction")
    mask_hierarchy = _active_mask("hierarchy")
    mask_collision = _active_mask("collision")
    mask_tier = _active_mask("tier")
    mask_gravity = _active_mask("gravity")

    # Set random seed for reproducibility across runs with the same config
    np.random.seed(config["seed"])

    width = config["width"]
    height = config["height"]

    # Get tier target X positions — each node is attracted to the X column of its tier
    tier_x = np.array(
        [
            tiers[node["tier"]]["x_percent"] * width
            if node["tier"] < len(tiers)
            else 0.5 * width
            for node in nodes
        ]
    )

    # Initialize positions: X near tier column, Y spread across canvas with noise
    # to break symmetry and help the simulation converge to a good layout
    initial_y_noise = config.get("initial_y_noise", 200)
    positions = np.zeros((n, 2))
    positions[:, 0] = tier_x + np.random.uniform(-20, 20, n)
    base_y = np.linspace(height * 0.15, height * 0.85, n)
    np.random.shuffle(base_y)
    positions[:, 1] = base_y + np.random.uniform(-initial_y_noise, initial_y_noise, n)
    positions[:, 1] = np.clip(positions[:, 1], 50, height - 50)

    # Initialize velocities (zero — forces will accelerate nodes from rest)
    velocities = np.zeros((n, 2))

    # Build edge arrays for vectorized force computations
    flux_edges = []
    for link in links:
        src_idx = id_to_idx.get(link["source"])
        tgt_idx = id_to_idx.get(link["target"])
        if src_idx is not None and tgt_idx is not None:
            flux_edges.append((src_idx, tgt_idx, link.get("value", 1)))

    struct_edges = []
    for link in struct_links:
        src_idx = id_to_idx.get(link["source"])
        tgt_idx = id_to_idx.get(link["target"])
        if src_idx is not None and tgt_idx is not None:
            struct_edges.append((src_idx, tgt_idx, link.get("depth", 1)))

    # Node radii scale with model count — large folders take up more space
    node_counts = np.array([node.get("count", 1) for node in nodes], dtype=float)
    node_radii = config["collision_radius"] + np.sqrt(node_counts) * 2

    # Extract force parameters — separate X/Y strengths allow asymmetric layouts
    # (e.g. strong horizontal tier lock, free vertical positioning)
    repulsion_x = config.get("repulsion_x", config.get("node_repulsion", 200))
    repulsion_y = config.get("repulsion_y", config.get("node_repulsion", 200))
    attraction_x = config.get("attraction_x", config.get("flux_strength", 1.0))
    attraction_y = config.get("attraction_y", config.get("flux_strength", 1.0))
    collision_strength = config.get("collision_strength", 1.0)

    # ── Simulation loop ──────────────────────────────────────────────────────────
    iterations = int(config["iterations"])
    for iteration in range(iterations):
        forces = np.zeros((n, 2))

        # Force 1 — REPULSION (all node pairs, Coulomb's law: F ∝ 1/d²)
        # Pushes every node away from every other node to prevent clustering.
        # Separate X/Y strengths: high repulsion_y spreads nodes vertically
        # while repulsion_x keeps horizontal spread tight within each tier column.
        repulsion_forces = compute_repulsion_xy(
            positions, node_radii, repulsion_x, repulsion_y
        )
        repulsion_forces[~mask_repulsion] = 0
        forces += repulsion_forces

        # Force 2 — ATTRACTION along flux edges (Hooke's law: F ∝ displacement)
        # Connected nodes (data dependencies) pull toward each other like springs.
        # attraction_y=0 by default keeps edges horizontal — dependencies flow
        # left-to-right without pulling nodes up or down.
        attraction_forces = compute_attraction_xy(
            positions, flux_edges, attraction_x, attraction_y, config["flux_distance"]
        )
        attraction_forces[~mask_attraction] = 0
        forces += attraction_forces

        # Force 3 — HIERARCHY (structural folder edges, depth-weighted)
        # Nodes in the same folder subtree attract each other proportionally
        # to their shared path depth. This clusters sibling models together
        # and keeps related folders visually adjacent.
        hierarchy_forces = compute_hierarchy(
            positions,
            struct_edges,
            config["hierarchy_strength"],
            config["hierarchy_distance"],
        )
        hierarchy_forces[~mask_hierarchy] = 0
        forces += hierarchy_forces

        # Force 4 — COLLISION (hard body repulsion within node radius)
        # Prevents overlapping circles. Acts only when nodes are closer than
        # the sum of their radii — acts as a short-range complement to repulsion.
        collision_forces = compute_collision(positions, node_radii, collision_strength)
        collision_forces[~mask_collision] = 0
        forces += collision_forces

        # Force 5 — TIER CONSTRAINT (horizontal spring toward tier column)
        # Each node is pulled back to its assigned tier's X position.
        # tier_stiffness controls how strictly nodes stay in their columns —
        # high values produce clean vertical lanes, low values allow drift.
        tier_force = (tier_x - positions[:, 0]) * config["tier_stiffness"]
        forces[:, 0] += np.where(mask_tier, tier_force, 0)

        # Force 6 — CENTER GRAVITY (vertical pull toward canvas center)
        # Prevents nodes from drifting to the top or bottom edges over time.
        # Low strength (≈0.05) acts as a soft anchor without distorting layout.
        center_y = height / 2
        gravity_force = (center_y - positions[:, 1]) * config["center_gravity_y"]
        forces[:, 1] += np.where(mask_gravity, gravity_force, 0)

        # ── Velocity integration with damping ───────────────────────────────────
        # Euler integration: v = v * damping + F * dt  (dt=0.1 implicit)
        # Damping dissipates energy so the simulation converges instead of
        # oscillating forever. Values close to 1.0 = slow convergence, slow decay;
        # values close to 0.0 = fast decay, stiff layout.
        velocities = velocities * config["damping"] + forces * 0.1

        # Clamp velocity magnitude to prevent numerical explosions when forces
        # are very large early in the simulation
        max_velocity = 50
        velocity_magnitude = np.linalg.norm(velocities, axis=1, keepdims=True)
        velocity_magnitude = np.maximum(velocity_magnitude, 1e-6)
        velocities = np.where(
            velocity_magnitude > max_velocity,
            velocities * max_velocity / velocity_magnitude,
            velocities,
        )

        # Update positions
        positions += velocities

        # Hard boundary: keep all nodes within the canvas margins
        margin = 50
        positions[:, 0] = np.clip(positions[:, 0], margin, width - margin)
        positions[:, 1] = np.clip(positions[:, 1], margin, height - margin)

    # Ensure no NaN or Inf values
    positions = np.nan_to_num(positions, nan=width / 2, posinf=width / 2, neginf=0)

    return positions.tolist()


def compute_repulsion_xy(
    positions: np.ndarray, radii: np.ndarray, strength_x: float, strength_y: float
) -> np.ndarray:
    """
    Compute repulsion forces between all node pairs with separate X/Y strengths.

    Uses Coulomb's law: F = k / distance^2
    """
    n = len(positions)
    forces = np.zeros((n, 2))

    for i in range(n):
        # Vector from i to all other nodes
        diff = positions - positions[i]
        distances = np.linalg.norm(diff, axis=1)

        # Avoid self-interaction and division by zero
        distances[i] = 1e6
        distances = np.maximum(distances, 1.0)

        # Combined radii for collision detection
        min_dist = radii[i] + radii
        distances = np.maximum(distances, min_dist * 0.5)

        # Repulsion magnitude per axis (positive strength = push apart)
        mag_x = strength_x / (distances**2)
        mag_y = strength_y / (distances**2)

        # Direction: diff points FROM node i TO other nodes
        # Normalize each component separately
        dir_x = np.where(np.abs(diff[:, 0]) > 1e-6, -diff[:, 0] / distances, 0)
        dir_y = np.where(np.abs(diff[:, 1]) > 1e-6, -diff[:, 1] / distances, 0)

        # Apply repulsion force (negate to push apart)
        force_x = -mag_x * dir_x
        force_y = -mag_y * dir_y

        force_x[i] = 0
        force_y[i] = 0

        forces[i, 0] = force_x.sum()
        forces[i, 1] = force_y.sum()

    return forces


def compute_attraction_xy(
    positions: np.ndarray,
    edges: list[tuple[int, int, float]],
    strength_x: float,
    strength_y: float,
    ideal_distance: float,
) -> np.ndarray:
    """
    Compute attraction forces along edges with separate X/Y strengths.

    Uses Hooke's law: F = k * (distance - ideal_distance)
    """
    n = len(positions)
    forces = np.zeros((n, 2))

    for src_idx, tgt_idx, weight in edges:
        diff = positions[tgt_idx] - positions[src_idx]
        distance = np.linalg.norm(diff)

        if distance < 1e-6:
            continue

        # Spring force
        displacement = distance - ideal_distance
        weight_factor = np.sqrt(weight)

        # Direction (normalized)
        dir_x = diff[0] / distance
        dir_y = diff[1] / distance

        # Apply force per axis with separate strengths
        force_x = strength_x * displacement * weight_factor * dir_x
        force_y = strength_y * displacement * weight_factor * dir_y

        forces[src_idx, 0] += force_x
        forces[src_idx, 1] += force_y
        forces[tgt_idx, 0] -= force_x
        forces[tgt_idx, 1] -= force_y

    return forces


def compute_collision(
    positions: np.ndarray, radii: np.ndarray, strength: float
) -> np.ndarray:
    """
    Compute collision forces to prevent node overlap.

    Pushes nodes apart when they're closer than the sum of their radii.
    """
    n = len(positions)
    forces = np.zeros((n, 2))

    for i in range(n):
        diff = positions - positions[i]
        distances = np.linalg.norm(diff, axis=1)

        # Avoid self-interaction
        distances[i] = 1e6

        # Sum of radii (minimum allowed distance)
        min_distances = radii[i] + radii

        # Only apply force when overlapping (distance < min_distance)
        overlap = min_distances - distances
        overlap = np.maximum(overlap, 0)  # Only positive overlaps

        # Force magnitude proportional to overlap
        magnitude = strength * overlap

        # Direction: push away from other nodes
        safe_dist = np.maximum(distances, 1e-6)
        direction = -diff / safe_dist[:, np.newaxis]

        # Apply force
        force = magnitude[:, np.newaxis] * direction
        force[i] = 0
        forces[i] = force.sum(axis=0)

    return forces


# Keep old functions for backward compatibility
def compute_repulsion(positions, radii, strength):
    return compute_repulsion_xy(positions, radii, strength, strength)


def compute_attraction(positions, edges, strength, ideal_distance):
    return compute_attraction_xy(positions, edges, strength, strength, ideal_distance)


def compute_hierarchy(
    positions: np.ndarray,
    struct_edges: list[tuple[int, int, int]],
    strength: float,
    ideal_distance: float,
) -> np.ndarray:
    """
    Compute hierarchy forces between structurally related nodes.

    Strength scales with shared depth squared.
    """
    n = len(positions)
    forces = np.zeros((n, 2))

    for src_idx, tgt_idx, depth in struct_edges:
        diff = positions[tgt_idx] - positions[src_idx]
        distance = np.linalg.norm(diff)

        if distance < 1e-6:
            continue

        # Strength proportional to depth^2
        effective_strength = strength * (depth**2)
        displacement = distance - ideal_distance

        # Only attract if beyond ideal distance
        if displacement > 0:
            magnitude = effective_strength * displacement
            direction = diff / distance
            force = magnitude * direction
            forces[src_idx] += force
            forces[tgt_idx] -= force

    return forces


def compute_layout_from_viz_data(
    viz_data: dict,
    config: dict | None = None,
    disabled_forces: dict[str, list[str]] | None = None,
) -> dict:
    """
    Compute layout and add positions to viz_data.

    Args:
        viz_data: Dictionary with nodes, links, struct_links, tiers
        config: Force parameters
        disabled_forces: Optional dict mapping force names to node IDs to exclude.
            See compute_layout() for full documentation.

    Returns:
        viz_data with x, y coordinates added to each node
    """
    positions = compute_layout(
        nodes=viz_data["nodes"],
        links=viz_data["links"],
        struct_links=viz_data.get("struct_links", []),
        tiers=viz_data["tiers"],
        config=config,
        disabled_forces=disabled_forces,
    )

    # Add positions to nodes
    for i, node in enumerate(viz_data["nodes"]):
        node["x"] = positions[i][0]
        node["y"] = positions[i][1]

    return viz_data


def generate_layout(
    input_path: str,
    output_path: str,
    config: dict | None = None,
    disabled_forces: dict[str, list[str]] | None = None,
    **context: Any,
) -> str:
    """
    Load viz_data.json, compute layout, and save with positions.

    This function is designed to be called as an Airflow PythonOperator callable.

    Args:
        input_path: Path to viz_data.json
        output_path: Path to write viz_data_with_layout.json
        config: Force parameters (optional, loads from config.yaml if not provided)
        disabled_forces: Optional dict mapping force names to node IDs to exclude.
            Example: {"tier": ["src.raw"], "gravity": ["export"]}
        **context: Airflow context (unused but required for PythonOperator)

    Returns:
        Path to the generated file
    """
    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Load viz data
    with open(input_path, "r") as f:
        viz_data = json.load(f)

    # Load config from YAML if not provided
    if config is None:
        config = load_config_from_yaml()

    # Compute layout
    viz_data = compute_layout_from_viz_data(viz_data, config, disabled_forces)

    # Update metadata
    if "metadata" not in viz_data:
        viz_data["metadata"] = {}
    viz_data["metadata"]["layout_computed"] = True

    # Write output
    with open(output_path, "w") as f:
        json.dump(viz_data, f, indent=2)

    print(f"Computed layout for {len(viz_data['nodes'])} nodes")
    return output_path


# Note: __main__ block removed for Pyodide compatibility
# For CLI testing, use: python -c "from layout_algorithm import generate_layout; generate_layout('input.json', 'output.json')"
