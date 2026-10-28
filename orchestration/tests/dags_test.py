import fnmatch
import re
import tomllib
import unittest
from datetime import timedelta
from pathlib import Path
from unittest import mock

import pandas as pd
from airflow.models import DagBag
from common.operators.gce import InstallDependenciesOperator
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from jobs.crons import SCHEDULE_DICT

DAG_ID_LIST = list(SCHEDULE_DICT.keys())

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Same pattern as TEST_DAG_FILE_REGEX_PATTERN in
# jobs/administration/purge_test_dags.py: local/ephemeral DAG copies created
# by `make create-test-dag`, never committed, so they're exempt from checks
# that only matter for real, deployed DAGs.
TEST_DAG_FILE_PATTERN = "*__test__*.py"

# DAGs whose InstallDependenciesOperator base_dir/python_version are freely
# chosen by whoever triggers the run (not pinned to one microservice), so
# there is nothing fixed to check against pyproject.toml.
INSTALL_DEPENDENCIES_CHECK_SKIP_DAG_IDS = {"gcp_launch_vm"}


def _strip_matching_quotes(value: str) -> str:
    # Some DAGs set `render_template_as_native_obj=True`, under which Jinja
    # literal_eval's the rendered string (so "3.10" would become the float
    # 3.1). Those DAGs guard against this by writing the value as "'3.10'",
    # which literal_eval turns back into the string "3.10". Unwrap that here
    # so both styles compare equal.
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _resolve_templated_param(value: str, dag) -> str | None:
    match = re.fullmatch(r"\{\{\s*params\.(\w+)\s*\}\}", value)
    if not match:
        return None
    param_name = match.group(1)
    if param_name not in dag.params:
        return None
    return dag.params[param_name]


def _resolve_install_dependencies_field(value: str, dag) -> str | None:
    if "{{" in value:
        value = _resolve_templated_param(value, dag)
        if value is None:
            return None
    return _strip_matching_quotes(value)


class TestDags(unittest.TestCase):
    LOAD_SECOND_THRESHOLD = timedelta(seconds=2)

    def setUp(self):
        with (
            mock.patch(
                "common.bigquery_client.BigQueryClient.query"
            ) as bigquery_mocker,
            mock.patch(
                "common.access_gcp_secrets.access_secret_data"
            ) as access_secret_mocker,
        ):

            def bigquery_client(query):
                return (
                    pd.DataFrame(
                        {"f0_": [pd.to_datetime(1490195805, unit="s", utc=True)]}
                    )
                    if "SELECT max(visit_last_action_time) FROM" in query
                    else pd.DataFrame({0: [0]})
                )

            access_secret_mocker.return_value = "{}"
            bigquery_mocker.side_effect = bigquery_client
            self.dagbag = DagBag(include_examples=False)

    def test_dag_import_no_error(self):
        # Then
        self.assertFalse(
            len(self.dagbag.import_errors),
            "There should be no DAG failures. Got: {}".format(
                self.dagbag.import_errors
            ),
        )

    def test_dag_import_is_fast(self):
        # Given
        stats = self.dagbag.dagbag_stats
        slow_files = filter(lambda d: d.duration > self.LOAD_SECOND_THRESHOLD, stats)
        res = ", ".join(map(lambda d: d.file[1:], slow_files))

        # When / Then
        self.assertEqual(
            0,
            len(list(slow_files)),
            "The following files take more than {threshold}s to load: {res}".format(
                threshold=self.LOAD_SECOND_THRESHOLD, res=res
            ),
        )

    def test_dags_are_loaded(self):
        for dag_id in DAG_ID_LIST:
            dag = self.dagbag.get_dag(dag_id=dag_id)
            self.assertIsNotNone(dag, f"DAG {dag_id} should exist.")

    def test_install_dependencies_python_version_matches_pyproject(self):
        errors = []

        for dag in self.dagbag.dags.values():
            if dag.dag_id in INSTALL_DEPENDENCIES_CHECK_SKIP_DAG_IDS:
                continue
            if fnmatch.fnmatch(Path(dag.fileloc).name, TEST_DAG_FILE_PATTERN):
                continue

            for task in dag.tasks:
                if not isinstance(task, InstallDependenciesOperator):
                    continue

                location = f"{dag.dag_id}.{task.task_id}"
                python_version = _resolve_install_dependencies_field(
                    task.python_version, dag
                )
                base_dir = _resolve_install_dependencies_field(task.base_dir, dag)

                if python_version is None or base_dir is None:
                    errors.append(
                        f"{location}: could not statically resolve python_version "
                        f"({task.python_version!r}) or base_dir ({task.base_dir!r}). "
                        "Add the dag_id to INSTALL_DEPENDENCIES_CHECK_SKIP_DAG_IDS "
                        "if this is expected."
                    )
                    continue

                if base_dir == "data-gcp":
                    service_dir = REPO_ROOT
                elif base_dir.startswith("data-gcp/"):
                    service_dir = REPO_ROOT / base_dir[len("data-gcp/") :].rstrip("/")
                else:
                    errors.append(
                        f"{location}: base_dir {base_dir!r} is not rooted at "
                        "'data-gcp'."
                    )
                    continue

                pyproject_path = service_dir / "pyproject.toml"
                if not pyproject_path.is_file():
                    errors.append(
                        f"{location}: no pyproject.toml found at {pyproject_path} "
                        f"(base_dir={base_dir!r})."
                    )
                    continue

                with pyproject_path.open("rb") as f:
                    pyproject = tomllib.load(f)
                requires_python = pyproject.get("project", {}).get("requires-python")
                if not requires_python:
                    # Not every job declares requires-python yet; nothing to check.
                    continue

                try:
                    is_compatible = Version(python_version) in SpecifierSet(
                        requires_python
                    )
                except (InvalidVersion, InvalidSpecifier) as exc:
                    errors.append(f"{location}: {exc}")
                    continue

                if not is_compatible:
                    errors.append(
                        f"{location}: InstallDependenciesOperator uses "
                        f"python_version={python_version!r} but "
                        f"{pyproject_path.relative_to(REPO_ROOT)} requires "
                        f"{requires_python!r}."
                    )

        self.assertFalse(
            errors,
            "InstallDependenciesOperator.python_version must be explicitly set "
            "and compatible with the target microservice's pyproject.toml "
            "requires-python:\n" + "\n".join(errors),
        )
