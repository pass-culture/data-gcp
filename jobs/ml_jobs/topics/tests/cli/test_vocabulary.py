import json
import pathlib

import polars as pl
from typer.testing import CliRunner

from cli import vocabulary as vocabulary_cli

runner = CliRunner()

BOILERPLATE = (
    "Cette édition a été réalisée en partenariat avec la BnF dans le cadre "
    "de son programme de numérisation."
)

FRENCH_DESCRIPTIONS = [
    "Un roman passionnant qui raconte une histoire originale et pleine de "
    "rebondissements.",
    "Une biographie détaillée et fascinante d'un grand personnage historique français.",
]


def _items_df(item_ids: list[str], descriptions: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "item_id": item_ids,
            "gtl_type": ["BOOK"] * len(item_ids),
            "gtl_id": ["01000000"] * len(item_ids),
            "gtl_label_level_1": ["Littérature"] * len(item_ids),
            "offer_description": descriptions,
        }
    )


def test_find_boilerplate_writes_segments_json(tmp_path: pathlib.Path) -> None:
    descriptions = [f"Roman numéro {i}. {BOILERPLATE}" for i in range(50)] + [
        f"Une histoire originale numéro {i}." for i in range(50)
    ]
    input_path = tmp_path / "items.parquet"
    pl.DataFrame({"offer_description": descriptions}).write_parquet(input_path)
    output_path = tmp_path / "boilerplate.json"

    result = runner.invoke(
        vocabulary_cli.app,
        [
            "find-boilerplate",
            "--input-file-path",
            str(input_path),
            "--output-file-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    segments = json.loads(output_path.read_text())
    assert BOILERPLATE in segments


def test_select_delta_first_run_selects_all_items(tmp_path: pathlib.Path) -> None:
    input_path = tmp_path / "items.parquet"
    _items_df(["1", "2"], FRENCH_DESCRIPTIONS).write_parquet(input_path)
    boilerplate_path = tmp_path / "boilerplate.json"
    boilerplate_path.write_text("[]")
    output_path = tmp_path / "delta.parquet"

    result = runner.invoke(
        vocabulary_cli.app,
        [
            "select-delta",
            "--input-file-path",
            str(input_path),
            "--boilerplate-file-path",
            str(boilerplate_path),
            "--output-file-path",
            str(output_path),
            "--processing-version",
            "v1",
        ],
    )

    assert result.exit_code == 0, result.output
    delta = pl.read_parquet(output_path)
    assert set(delta["item_id"].to_list()) == {"1", "2"}
    assert not delta["is_deleted"].any()


def test_select_delta_skips_unchanged_item_on_second_run(
    tmp_path: pathlib.Path,
) -> None:
    input_path = tmp_path / "items.parquet"
    _items_df(["1", "2"], FRENCH_DESCRIPTIONS).write_parquet(input_path)
    boilerplate_path = tmp_path / "boilerplate.json"
    boilerplate_path.write_text("[]")

    first_output = tmp_path / "delta_1.parquet"
    runner.invoke(
        vocabulary_cli.app,
        [
            "select-delta",
            "--input-file-path",
            str(input_path),
            "--boilerplate-file-path",
            str(boilerplate_path),
            "--output-file-path",
            str(first_output),
            "--processing-version",
            "v1",
        ],
    )
    previous_documents_path = tmp_path / "previous.parquet"
    pl.read_parquet(first_output).select("item_id", "description_hash").write_parquet(
        previous_documents_path
    )

    changed_input_path = tmp_path / "items_v2.parquet"
    _items_df(
        ["1", "2"],
        [
            FRENCH_DESCRIPTIONS[0],
            "Une toute nouvelle histoire complètement différente de la précédente.",
        ],
    ).write_parquet(changed_input_path)
    second_output = tmp_path / "delta_2.parquet"

    result = runner.invoke(
        vocabulary_cli.app,
        [
            "select-delta",
            "--input-file-path",
            str(changed_input_path),
            "--boilerplate-file-path",
            str(boilerplate_path),
            "--output-file-path",
            str(second_output),
            "--processing-version",
            "v1",
            "--previous-documents-file-path",
            str(previous_documents_path),
            "--previous-processing-version",
            "v1",
        ],
    )

    assert result.exit_code == 0, result.output
    delta = pl.read_parquet(second_output)
    assert delta.filter(~pl.col("is_deleted"))["item_id"].to_list() == ["2"]


def test_select_delta_processing_version_bump_reprocesses_everything(
    tmp_path: pathlib.Path,
) -> None:
    input_path = tmp_path / "items.parquet"
    _items_df(["1", "2"], FRENCH_DESCRIPTIONS).write_parquet(input_path)
    boilerplate_path = tmp_path / "boilerplate.json"
    boilerplate_path.write_text("[]")

    first_output = tmp_path / "delta_1.parquet"
    runner.invoke(
        vocabulary_cli.app,
        [
            "select-delta",
            "--input-file-path",
            str(input_path),
            "--boilerplate-file-path",
            str(boilerplate_path),
            "--output-file-path",
            str(first_output),
            "--processing-version",
            "v1",
        ],
    )
    previous_documents_path = tmp_path / "previous.parquet"
    pl.read_parquet(first_output).select("item_id", "description_hash").write_parquet(
        previous_documents_path
    )

    second_output = tmp_path / "delta_2.parquet"
    result = runner.invoke(
        vocabulary_cli.app,
        [
            "select-delta",
            "--input-file-path",
            str(input_path),
            "--boilerplate-file-path",
            str(boilerplate_path),
            "--output-file-path",
            str(second_output),
            "--processing-version",
            "v2",
            "--previous-documents-file-path",
            str(previous_documents_path),
            "--previous-processing-version",
            "v1",
        ],
    )

    assert result.exit_code == 0, result.output
    delta = pl.read_parquet(second_output)
    assert set(delta.filter(~pl.col("is_deleted"))["item_id"].to_list()) == {"1", "2"}


def test_process_builds_documents_and_flags_empty_and_deleted(
    tmp_path: pathlib.Path,
) -> None:
    delta = pl.DataFrame(
        {
            "item_id": ["1", "2", "3"],
            "gtl_id": ["01010000", "01020000", "02000000"],
            "cleaned_description": [
                "Les chats aimaient jouer avec les enfants.",
                "",
                None,
            ],
            "is_deleted": [False, False, True],
        }
    )
    input_path = tmp_path / "delta.parquet"
    delta.write_parquet(input_path)
    output_path = tmp_path / "processed.parquet"

    result = runner.invoke(
        vocabulary_cli.app,
        [
            "process",
            "--input-file-path",
            str(input_path),
            "--output-file-path",
            str(output_path),
            "--processing-version",
            "v1",
        ],
    )

    assert result.exit_code == 0, result.output
    processed = pl.read_parquet(output_path)

    row1 = processed.filter(pl.col("item_id") == "1").row(0, named=True)
    assert "chat" in row1["noun_lemmas"]
    assert "enfant" in row1["noun_lemmas"]
    assert row1["node_id"] == "01000000"
    assert row1["is_empty"] is False
    assert row1["processing_version"] == "v1"

    row2 = processed.filter(pl.col("item_id") == "2").row(0, named=True)
    assert row2["is_empty"] is True

    row3 = processed.filter(pl.col("item_id") == "3").row(0, named=True)
    assert row3["is_deleted"] is True
    assert row3["processing_version"] == "v1"
